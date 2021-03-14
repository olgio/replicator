package ru.splite.replicator

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.executor.CommandExecutor
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import ru.splite.replicator.transport.sender.MessageSender

class AtlasCommandSubmitter(
    val atlasProtocol: AtlasProtocolController,
    private val coroutineScopeToSendCommit: CoroutineScope,
    private val commandExecutor: CommandExecutor
) : StateMachineCommandSubmitter<ByteArray, ByteArray> {

    private val messageSender = MessageSender(atlasProtocol, atlasProtocol.config.sendMessageTimeout)

    override suspend fun submit(command: ByteArray): ByteArray {
        val commandCoordinator = atlasProtocol.createCommandCoordinator()
        return withTimeout(atlasProtocol.config.commandExecutorTimeout) {
            commandExecutor.awaitCommandResponse(commandCoordinator.commandId) {
                val commitMessage = coordinateCommand(commandCoordinator, command)
                check(!commitMessage.value.isNoop) //TODO
            }
        }
    }

    private suspend fun coordinateCommand(
        commandCoordinator: CommandCoordinator,
        command: ByteArray
    ): AtlasMessage.MCommit {
        val fastQuorumNodes = messageSender.getNearestNodes(atlasProtocol.config.fastQuorumSize)

        val collectMessage = commandCoordinator.buildCollect(command, fastQuorumNodes)

        val collectAckDecision = messageSender.sendToAllOrThrow(fastQuorumNodes) {
            collectMessage
        }.map {
            commandCoordinator.handleCollectAck(it.dst, it.response as AtlasMessage.MCollectAck)
        }.firstOrNull {
            it != CommandCoordinator.CollectAckDecision.NONE
        } ?: CommandCoordinator.CollectAckDecision.NONE

        if (collectAckDecision == CommandCoordinator.CollectAckDecision.COMMIT) {
            return sendCommitToAllExternalContext(commandCoordinator, fastQuorumNodes)
        } else if (collectAckDecision == CommandCoordinator.CollectAckDecision.CONFLICT) {
            LOGGER.debug("Chosen slow path. commandId=${commandCoordinator.commandId}")
            val slowQuorumNodes = messageSender.getNearestNodes(atlasProtocol.config.slowQuorumSize)
            val consensusMessage = commandCoordinator.buildConsensus()

            messageSender.sendToAllOrThrow(slowQuorumNodes) {
                consensusMessage
            }.map {
                commandCoordinator.handleConsensusAck(it.dst, it.response as AtlasMessage.MConsensusAck)
            }.firstOrNull {
                it == CommandCoordinator.ConsensusAckDecision.COMMIT
            } ?: error("Slow quorum invariant violated")

            return sendCommitToAllExternalContext(commandCoordinator, fastQuorumNodes)
        } else {
            error("Cannot achieve consensus for command ${commandCoordinator.commandId}: collectAckDecision = $collectAckDecision")
        }
    }

    private fun sendCommitToAllExternalContext(
        commandCoordinator: CommandCoordinator,
        fastQuorumNodes: Collection<NodeIdentifier>
    ): AtlasMessage.MCommit {
        val commitForFastPath = commandCoordinator.buildCommit(false)
        val commitWithPayload = commandCoordinator.buildCommit(true)

        coroutineScopeToSendCommit.launch {
            LOGGER.debug("Sending commits async. commandId=${commandCoordinator.commandId}")
            val successCommitsSize = messageSender.getAllNodes().map {
                async {
                    messageSender.sendOrNull(
                        it,
                        if (fastQuorumNodes.contains(it)) commitForFastPath else commitWithPayload
                    )
                }
            }.mapNotNull { it.await() }.size
            LOGGER.debug("Successfully sent $successCommitsSize commits. commandId=${commandCoordinator.commandId}")
        }
        return commitForFastPath
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}