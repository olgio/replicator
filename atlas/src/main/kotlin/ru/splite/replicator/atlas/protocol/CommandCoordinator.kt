package ru.splite.replicator.atlas.protocol

import ru.splite.replicator.atlas.AtlasMessage
import ru.splite.replicator.atlas.id.Id
import ru.splite.replicator.transport.NodeIdentifier

interface CommandCoordinator {

    enum class CollectAckDecision { COMMIT, CONFLICT, NONE }

    enum class ConsensusAckDecision { COMMIT, NONE }

    val commandId: Id<NodeIdentifier>

    suspend fun buildCollect(
        command: ByteArray,
        fastQuorumNodes: Set<NodeIdentifier>,
        handle: Boolean = true
    ): AtlasMessage.MCollect

    suspend fun buildConsensus(handle: Boolean = true): AtlasMessage.MConsensus

    suspend fun buildCommit(withPayload: Boolean = false, handle: Boolean = true): AtlasMessage.MCommit

    suspend fun buildRecovery(): AtlasMessage.MRecovery

    suspend fun handleCollectAck(from: NodeIdentifier, collectAck: AtlasMessage.MCollectAck): CollectAckDecision

    suspend fun handleConsensusAck(from: NodeIdentifier, consensusAck: AtlasMessage.MConsensusAck): ConsensusAckDecision

    suspend fun handleRecoveryAck(
        from: NodeIdentifier,
        recoveryAck: AtlasMessage.MRecoveryAck
    ): AtlasMessage.MConsensus?
}