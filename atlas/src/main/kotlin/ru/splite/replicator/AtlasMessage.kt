package ru.splite.replicator

import kotlinx.serialization.Serializable
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.id.Id

@Serializable
sealed class AtlasMessage {

    @Serializable
    data class ConsensusValue(
        val isNoop: Boolean,
        val dependencies: Set<Dependency> = emptySet()
    )

    @Serializable
    data class MCollect(
        val commandId: Id<NodeIdentifier>,
        val command: ByteArray,
        val quorum: Set<NodeIdentifier> = emptySet(),
        val remoteDependencies: Set<Dependency> = emptySet()
    ) : AtlasMessage() {

        override fun toString(): String {
            return "MCollect(commandId=$commandId, quorum=$quorum, remoteDependencies=$remoteDependencies)"
        }
    }

    @Serializable
    data class MCollectAck(
        val isAck: Boolean,
        val commandId: Id<NodeIdentifier>,
        val remoteDependencies: Set<Dependency> = emptySet()
    ) : AtlasMessage()

    @Serializable
    data class MCommit(
        val commandId: Id<NodeIdentifier>,
        val value: ConsensusValue,
        val command: ByteArray
    ) : AtlasMessage() {

        override fun toString(): String {
            return "MCommit(commandId=$commandId, value=$value)"
        }
    }

    @Serializable
    data class MCommitAck(
        val isAck: Boolean,
        val commandId: Id<NodeIdentifier>
    ) : AtlasMessage()

    @Serializable
    data class MConsensus(
        val commandId: Id<NodeIdentifier>,
        val ballot: Long,
        val consensusValue: ConsensusValue
    ) : AtlasMessage()

    @Serializable
    data class MConsensusAck(
        val isAck: Boolean,
        val commandId: Id<NodeIdentifier>,
        val ballot: Long
    ) : AtlasMessage()

    @Serializable
    data class MRecovery(
        val commandId: Id<NodeIdentifier>,
        val command: ByteArray,
        val ballot: Long
    ) : AtlasMessage()

    @Serializable
    data class MRecoveryAck(
        val isAck: Boolean,
        val commandId: Id<NodeIdentifier>,
        val consensusValue: ConsensusValue,
        val quorum: Set<NodeIdentifier> = emptySet(),
        val ballot: Long,
        val acceptedBallot: Long
    ) : AtlasMessage()
}
