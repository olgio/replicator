package ru.splite.replicator

import kotlinx.serialization.Serializable
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.id.Id
import ru.splite.replicator.transport.NodeIdentifier

@Serializable
sealed class AtlasMessage {

    /**
     * Вспомогательный интерфейс для сообщений специфичных конкретной команде
     */
    interface PerCommandMessage {
        val commandId: Id<NodeIdentifier>
    }

    @Serializable
    data class ConsensusValue(
        val isNoop: Boolean,
        val dependencies: Set<Dependency> = emptySet()
    )

    @Serializable
    data class MCollect(
        override val commandId: Id<NodeIdentifier>,
        val command: ByteArray,
        val quorum: Set<NodeIdentifier> = emptySet(),
        val remoteDependencies: Set<Dependency> = emptySet()
    ) : AtlasMessage(), PerCommandMessage {

        override fun toString(): String {
            return "MCollect(commandId=$commandId, quorum=$quorum, remoteDependencies=$remoteDependencies)"
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as MCollect

            if (commandId != other.commandId) return false
            if (quorum != other.quorum) return false
            if (remoteDependencies != other.remoteDependencies) return false

            return true
        }

        override fun hashCode(): Int {
            var result = commandId.hashCode()
            result = 31 * result + quorum.hashCode()
            result = 31 * result + remoteDependencies.hashCode()
            return result
        }
    }

    @Serializable
    data class MCollectAck(
        val isAck: Boolean,
        override val commandId: Id<NodeIdentifier>,
        val remoteDependencies: Set<Dependency> = emptySet()
    ) : AtlasMessage(), PerCommandMessage

    @Serializable
    data class MCommit(
        override val commandId: Id<NodeIdentifier>,
        val value: ConsensusValue,
        val command: ByteArray
    ) : AtlasMessage(), PerCommandMessage {

        override fun toString(): String {
            return "MCommit(commandId=$commandId, value=$value)"
        }

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as MCommit

            if (commandId != other.commandId) return false
            if (value != other.value) return false

            return true
        }

        override fun hashCode(): Int {
            var result = commandId.hashCode()
            result = 31 * result + value.hashCode()
            return result
        }
    }

    @Serializable
    data class MCommitAck(
        val isAck: Boolean,
        override val commandId: Id<NodeIdentifier>
    ) : AtlasMessage(), PerCommandMessage

    @Serializable
    data class MConsensus(
        override val commandId: Id<NodeIdentifier>,
        val ballot: Long,
        val consensusValue: ConsensusValue
    ) : AtlasMessage(), PerCommandMessage

    @Serializable
    data class MConsensusAck(
        val isAck: Boolean,
        override val commandId: Id<NodeIdentifier>,
        val ballot: Long
    ) : AtlasMessage(), PerCommandMessage

    @Serializable
    data class MRecovery(
        override val commandId: Id<NodeIdentifier>,
        val command: ByteArray,
        val ballot: Long
    ) : AtlasMessage(), PerCommandMessage {

        override fun equals(other: Any?): Boolean {
            if (this === other) return true
            if (javaClass != other?.javaClass) return false

            other as MRecovery

            if (commandId != other.commandId) return false
            if (ballot != other.ballot) return false

            return true
        }

        override fun hashCode(): Int {
            var result = commandId.hashCode()
            result = 31 * result + ballot.hashCode()
            return result
        }
    }

    @Serializable
    data class MRecoveryAck(
        val isAck: Boolean,
        override val commandId: Id<NodeIdentifier>,
        val consensusValue: ConsensusValue,
        val quorum: Set<NodeIdentifier> = emptySet(),
        val ballot: Long,
        val acceptedBallot: Long
    ) : AtlasMessage(), PerCommandMessage
}
