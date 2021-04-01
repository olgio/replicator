package ru.splite.replicator

import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.id.Id

interface CommandCoordinator {

    enum class CollectAckDecision { COMMIT, CONFLICT, NONE }

    enum class ConsensusAckDecision { COMMIT, NONE }

    val commandId: Id<NodeIdentifier>

    fun buildCollect(command: ByteArray, fastQuorumNodes: Set<NodeIdentifier>): AtlasMessage.MCollect

    fun buildConsensus(): AtlasMessage.MConsensus

    fun buildCommit(withPayload: Boolean = false): AtlasMessage.MCommit

    fun buildRecovery(): AtlasMessage.MRecovery

    fun handleCollectAck(from: NodeIdentifier, collectAck: AtlasMessage.MCollectAck): CollectAckDecision

    fun handleConsensusAck(from: NodeIdentifier, consensusAck: AtlasMessage.MConsensusAck): ConsensusAckDecision

    fun handleRecoveryAck(from: NodeIdentifier, recoveryAck: AtlasMessage.MRecoveryAck): AtlasMessage.MConsensus?
}