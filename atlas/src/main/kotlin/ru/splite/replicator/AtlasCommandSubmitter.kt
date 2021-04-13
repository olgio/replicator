package ru.splite.replicator

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.mapNotNull
import org.slf4j.LoggerFactory
import ru.splite.replicator.executor.CommandExecutor
import ru.splite.replicator.metrics.Metrics
import ru.splite.replicator.state.CommandState
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import ru.splite.replicator.timer.flow.TimerFactory
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.sender.MessageSender
import kotlin.coroutines.CoroutineContext

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
                val commitMessageResult = kotlin.runCatching {
                    coordinateCommand(commandCoordinator, command)
                }
                if (commitMessageResult.isFailure) {
                    LOGGER.error(
                        "Failed to coordinate commandId=${commandCoordinator.commandId}. Starting recovery",
                        commitMessageResult.exceptionOrNull()
                    )
                }
                val commitMessage = commitMessageResult.getOrNull()
                    ?: recoveryCommand(commandCoordinator)
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
    ): AtlasMessage.MCommit {
        val fastQuorumNodes = messageSender.getNearestNodes(atlasProtocol.config.fastQuorumSize)
        LOGGER.debug(
            "fastQuorumNodes=${fastQuorumNodes.map { it.identifier }}. " +
                    "commandId=${commandCoordinator.commandId}"
        )

        val collectMessage = commandCoordinator.buildCollect(command, fastQuorumNodes)

        val collectAckDecision = messageSender.sendToAllOrThrow(fastQuorumNodes) {
            collectMessage
        }.map {
            commandCoordinator.handleCollectAck(it.dst, it.response as AtlasMessage.MCollectAck)
        }.firstOrNull {
            it != CommandCoordinator.CollectAckDecision.NONE
        } ?: CommandCoordinator.CollectAckDecision.NONE

        return when (collectAckDecision) {
            CommandCoordinator.CollectAckDecision.COMMIT -> {
                sendCommitToAllExternalContext(commandCoordinator, fastQuorumNodes)
            }
            CommandCoordinator.CollectAckDecision.CONFLICT -> {
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
            LOGGER.debug(
                "Sending commits async. " +
                        "commandId=${commandCoordinator.commandId}"
            )
            val successCommitsSize = messageSender.getAllNodes().map {
                async {
                    messageSender.sendOrNull(
                        it,
                        if (fastQuorumNodes.contains(it)) commitForFastPath else commitWithPayload
                    )
                }
            }.mapNotNull { it.await() }.size
            LOGGER.debug(
                "Successfully sent $successCommitsSize commits. " +
                        "commandId=${commandCoordinator.commandId}"
            )
        }
        return commitForFastPath
    }

    private suspend fun sendConsensusMessage(
        commandCoordinator: CommandCoordinator,
        fastQuorumNodes: Collection<NodeIdentifier>,
        consensusMessage: AtlasMessage.MConsensus
    ): AtlasMessage.MCommit {
        val slowQuorumNodes = messageSender.getNearestNodes(atlasProtocol.config.slowQuorumSize)
        messageSender.sendToAllOrThrow(slowQuorumNodes) {
            consensusMessage
        }.map {
            commandCoordinator.handleConsensusAck(it.dst, it.response as AtlasMessage.MConsensusAck)
        }.firstOrNull {
            it == CommandCoordinator.ConsensusAckDecision.COMMIT
        } ?: error("Slow quorum invariant violated")

        return sendCommitToAllExternalContext(commandCoordinator, fastQuorumNodes)
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
    ): Job {
        return coroutineScope.launch(coroutineContext) {
            val delayedFlow = timerFactory.delayedFlow(
                commandExecutor.commandBlockersFlow,
                atlasProtocol.config.commandRecoveryDelay
            )
            delayedFlow.collect {
                launch {
                    try {
                        if (atlasProtocol.getCommandStatus(it) == CommandState.Status.COMMIT) {
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
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}