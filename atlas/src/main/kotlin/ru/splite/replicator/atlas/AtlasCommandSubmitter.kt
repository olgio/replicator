package ru.splite.replicator.atlas

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.mapNotNull
import org.slf4j.LoggerFactory
import ru.splite.replicator.atlas.executor.CommandExecutor
import ru.splite.replicator.atlas.protocol.AtlasProtocol
import ru.splite.replicator.atlas.protocol.CommandCoordinator
import ru.splite.replicator.atlas.state.CommandStatus
import ru.splite.replicator.metrics.Metrics
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import ru.splite.replicator.timer.flow.TimerFactory
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.sender.MessageSender
import kotlin.coroutines.CoroutineContext

/**
 * Реализация отправки команд на репликацию протоколом Atlas
 */
class AtlasCommandSubmitter(
    private val atlasProtocol: AtlasProtocol,
    private val messageSender: MessageSender<AtlasMessage>,
    private val coroutineScopeToSendCommit: CoroutineScope,
    private val commandExecutor: CommandExecutor
) : StateMachineCommandSubmitter<ByteArray, ByteArray> {

    override suspend fun submit(command: ByteArray): ByteArray {
        val commandCoordinator = atlasProtocol.createCommandCoordinator()
        LOGGER.debug("Started coordinating commandId=${commandCoordinator.commandId}")
        return withTimeout(atlasProtocol.config.commandExecutorTimeout) {
            val response = commandExecutor.awaitCommandResponse(commandCoordinator.commandId) {
                val commitMessage = kotlin.runCatching {
                    coordinateCommand(commandCoordinator, command)
                }.getOrElse {
                    if (atlasProtocol.config.enableRecovery) {
                        recoveryCommand(commandCoordinator)
                    } else {
                        LOGGER.error("Failed to coordinate commandId=${commandCoordinator.commandId}", it)
                        throw it
                    }
                }
                check(!commitMessage.value.isNoop) {
                    "Cannot commit command because chosen value is NOOP"
                }
            }
            LOGGER.debug("Successfully completed commandId=${commandCoordinator.commandId}")
            response
        }
    }

    private suspend fun coordinateCommand(
        commandCoordinator: CommandCoordinator,
        command: ByteArray
    ): AtlasMessage.MCommit = coroutineScope {
        val fastQuorumNodes = messageSender.getNearestNodes(atlasProtocol.config.fastQuorumSize)
        LOGGER.debug(
            "fastQuorumNodes=${fastQuorumNodes.map { it.identifier }}. " +
                    "commandId=${commandCoordinator.commandId}"
        )

        val collectMessage = commandCoordinator.buildCollect(command, fastQuorumNodes)

        val collectAckDecision = fastQuorumNodes.map {
            async {
                val collectAck = messageSender.sendOrThrow(it, collectMessage) as AtlasMessage.MCollectAck
                commandCoordinator.handleCollectAck(it, collectAck)
            }
        }.map {
            it.await()
        }.firstOrNull {
            it != CommandCoordinator.CollectAckDecision.NONE
        } ?: CommandCoordinator.CollectAckDecision.NONE

        when (collectAckDecision) {
            CommandCoordinator.CollectAckDecision.COMMIT -> {
                Metrics.registry.atlasFastPathCounter.increment()
                sendCommitToAllExternalContext(commandCoordinator, fastQuorumNodes)
            }
            CommandCoordinator.CollectAckDecision.CONFLICT -> {
                Metrics.registry.atlasSlowPathCounter.increment()
                LOGGER.debug("Chosen slow path. commandId=${commandCoordinator.commandId}")
                val consensusMessage = commandCoordinator.buildConsensus()
                sendConsensusMessage(commandCoordinator, fastQuorumNodes, consensusMessage)
            }
            else -> {
                error(
                    "Cannot achieve consensus for command ${commandCoordinator.commandId}: " +
                            "collectAckDecision = $collectAckDecision"
                )
            }
        }
    }

    private suspend fun sendCommitToAllExternalContext(
        commandCoordinator: CommandCoordinator,
        fastQuorumNodes: Collection<NodeIdentifier>
    ): AtlasMessage.MCommit {
        val commitForFastPath = commandCoordinator.buildCommit(withPayload = false)

        val commitWithPayload = commandCoordinator.buildCommit(withPayload = true)

        val coroutineName = CoroutineName("commit-${commandCoordinator.commandId}")
        coroutineScopeToSendCommit.launch(coroutineName) {
            LOGGER.debug("Sending commits async. commandId={}", commandCoordinator.commandId)
            val successCommitsSize = messageSender.getAllNodes().map {
                async {
                    messageSender.sendOrNull(
                        it,
                        if (fastQuorumNodes.contains(it)) commitForFastPath else commitWithPayload
                    )
                }
            }.mapNotNull { it.await() }.size
            LOGGER.debug("Successfully sent {} commits for {}", successCommitsSize, commandCoordinator.commandId)
        }
        return commitForFastPath
    }

    private suspend fun sendConsensusMessage(
        commandCoordinator: CommandCoordinator,
        fastQuorumNodes: Collection<NodeIdentifier>,
        consensusMessage: AtlasMessage.MConsensus
    ): AtlasMessage.MCommit = coroutineScope {
        val slowQuorumNodes = messageSender.getNearestNodes(atlasProtocol.config.slowQuorumSize)
        slowQuorumNodes.map {
            async {
                val consensusAck =
                    messageSender.sendOrThrow(it, consensusMessage) as AtlasMessage.MConsensusAck
                commandCoordinator.handleConsensusAck(it, consensusAck)
            }
        }.map {
            it.await()
        }.firstOrNull {
            it == CommandCoordinator.ConsensusAckDecision.COMMIT
        } ?: error("Slow quorum invariant violated")

        sendCommitToAllExternalContext(commandCoordinator, fastQuorumNodes)
    }

    private suspend fun recoveryCommand(commandCoordinator: CommandCoordinator): AtlasMessage.MCommit {
        LOGGER.debug("Started recovery commandId=${commandCoordinator.commandId}")
        Metrics.registry.atlasRecoveryCounter.increment()
        val recoveryMessage = commandCoordinator.buildRecovery()

        val decisionMessage = messageSender.sendToAllAsFlow(messageSender.getAllNodes()) {
            recoveryMessage
        }.mapNotNull {
            when (val response = it.response) {
                is AtlasMessage.MCommit -> {
                    response
                }
                is AtlasMessage.MRecoveryAck -> {
                    commandCoordinator.handleRecoveryAck(it.dst, response)
                }
                else -> null
            }
        }.filterNotNull().firstOrNull()
            ?: error("Cannot choose decision for recovery. commandId=${commandCoordinator.commandId}")

        LOGGER.debug(
            "Received decision $decisionMessage while recovery. " +
                    "commandId=${commandCoordinator.commandId}"
        )

        return when (decisionMessage) {
            is AtlasMessage.MCommit -> {
                val commitAck = atlasProtocol.handleCommit(decisionMessage)
                check(commitAck.isAck) {
                    "Received MCommit message while recovery but commit rejected"
                }
                LOGGER.debug(
                    "Completed recovery with ${decisionMessage}. " +
                            "commandId=${commandCoordinator.commandId}"
                )
                decisionMessage
            }
            is AtlasMessage.MConsensus -> {
                val commitMessage = sendConsensusMessage(commandCoordinator, emptyList(), decisionMessage)
                LOGGER.debug(
                    "Completed recovery with ${commitMessage}. " +
                            "commandId=${commandCoordinator.commandId}"
                )
                commitMessage
            }
            else -> error("Cannot recovery command ${commandCoordinator.commandId}")
        }
    }

    fun launchCommandRecoveryLoop(
        coroutineContext: CoroutineContext,
        coroutineScope: CoroutineScope,
        timerFactory: TimerFactory
    ): Job = coroutineScope.launch(coroutineContext) {
        if (!atlasProtocol.config.enableRecovery) {
            return@launch
        }
        LOGGER.info("Enabled command recovery")
        val delayedFlow = timerFactory.delayedFlow(
            commandExecutor.commandBlockersFlow,
            atlasProtocol.config.commandRecoveryDelay
        )
        delayedFlow.collect {
            launch {
                try {
                    if (atlasProtocol.getCommandStatus(it) == CommandStatus.COMMIT) {
                        LOGGER.debug("Command already committed. commandId=${it}")
                        return@launch
                    }
                    val commandCoordinator = atlasProtocol.createCommandCoordinator(it)
                    recoveryCommand(commandCoordinator)
                } catch (e: Exception) {
                    LOGGER.error("Error while command recovery. commandId=${it}", e)
                }
            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}