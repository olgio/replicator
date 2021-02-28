package ru.splite.replicator

import ru.splite.replicator.bus.NodeIdentifier

interface CommandCoordinator {

    enum class CollectAckDecision { COMMIT, CONFLICT, NONE }

    enum class ConsensusAckDecision { COMMIT, NONE }

    fun buildCollect(command: ByteArray, fastQuorumNodes: Set<NodeIdentifier>): AtlasMessage.MCollect

    fun handleCollectAck(from: NodeIdentifier, collectAck: AtlasMessage.MCollectAck): CollectAckDecision

    fun buildConsensus(): AtlasMessage.MConsensus

    fun handleConsensusAck(from: NodeIdentifier, consensusAck: AtlasMessage.MConsensusAck): ConsensusAckDecision

    fun buildCommit(): AtlasMessage.MCommit

    fun buildRecovery(): AtlasMessage.MRecovery

    fun handleRecoveryAck(from: NodeIdentifier, recoveryAck: AtlasMessage.MRecoveryAck): AtlasMessage.MConsensus?
}