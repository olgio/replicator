package ru.splite.replicator

import kotlinx.serialization.Serializable
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.dependency.Dependency
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
    ) : AtlasMessage()

    @Serializable
    data class MCollectAck(
        val commandId: Id<NodeIdentifier>,
        val remoteDependencies: Set<Dependency> = emptySet()
    ) : AtlasMessage()

    @Serializable
    data class MCommit(
        val commandId: Id<NodeIdentifier>,
        val value: ConsensusValue
    ) : AtlasMessage()

    @Serializable
    data class MConsensus(
        val commandId: Id<NodeIdentifier>,
        val ballot: Long,
        val consensusValue: ConsensusValue
    ) : AtlasMessage()

    @Serializable
    data class MConsensusAck(
        val commandId: Id<NodeIdentifier>,
        val ballot: Long
    ) : AtlasMessage()
}
