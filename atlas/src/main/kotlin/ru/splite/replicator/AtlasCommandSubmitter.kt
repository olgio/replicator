package ru.splite.replicator

import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import ru.splite.replicator.executor.CommandExecutor
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import ru.splite.replicator.transport.sender.MessageSender

class AtlasCommandSubmitter(
    val atlasProtocol: AtlasProtocolController,
    private val commandExecutor: CommandExecutor
) : StateMachineCommandSubmitter<ByteArray, ByteArray> {

    private val messageSender = MessageSender(atlasProtocol, 1000L)

    override suspend fun submit(command: ByteArray): ByteArray {
        val commandCoordinator = atlasProtocol.createCommandCoordinator()
        val commitMessage = coordinateCommand(commandCoordinator, command)
        check(!commitMessage.value.isNoop) //TODO
        return withTimeout(3000L) {
            commandExecutor.awaitCommandResponse(commandCoordinator.commandId, command)
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
            val commitMessage = commandCoordinator.buildCommit()
            messageSender.getAllNodes().forEach {
                messageSender.sendOrNull(it, commitMessage)
            }
            return commitMessage
        } else if (collectAckDecision == CommandCoordinator.CollectAckDecision.CONFLICT) {
            val slowQuorumNodes = messageSender.getNearestNodes(atlasProtocol.config.slowQuorumSize)
            val consensusMessage = commandCoordinator.buildConsensus()

            messageSender.sendToAllOrThrow(slowQuorumNodes) {
                consensusMessage
            }.map {
                commandCoordinator.handleConsensusAck(it.dst, it.response as AtlasMessage.MConsensusAck)
            }.firstOrNull {
                it == CommandCoordinator.ConsensusAckDecision.COMMIT
            } ?: error("Slow quorum invariant violated")

            val commitMessage = commandCoordinator.buildCommit()
            messageSender.getAllNodes().forEach {
                messageSender.sendOrNull(it, commitMessage)
            }
            return commitMessage
        } else {
            error("Cannot achieve consensus for command ${collectMessage.commandId}: collectAckDecision = $collectAckDecision")
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}