package ru.splite.replicator

import ru.splite.replicator.id.Id
import ru.splite.replicator.transport.NodeIdentifier

interface CommandCoordinator {

    enum class CollectAckDecision { COMMIT, CONFLICT, NONE }

    enum class ConsensusAckDecision { COMMIT, NONE }

    val commandId: Id<NodeIdentifier>

    suspend fun buildCollect(command: ByteArray, fastQuorumNodes: Set<NodeIdentifier>): AtlasMessage.MCollect

    suspend fun buildConsensus(): AtlasMessage.MConsensus

    suspend fun buildCommit(withPayload: Boolean = false): AtlasMessage.MCommit

    suspend fun buildRecovery(): AtlasMessage.MRecovery

    suspend fun handleCollectAck(from: NodeIdentifier, collectAck: AtlasMessage.MCollectAck): CollectAckDecision

    suspend fun handleConsensusAck(from: NodeIdentifier, consensusAck: AtlasMessage.MConsensusAck): ConsensusAckDecision

    suspend fun handleRecoveryAck(
        from: NodeIdentifier,
        recoveryAck: AtlasMessage.MRecoveryAck
    ): AtlasMessage.MConsensus?
}