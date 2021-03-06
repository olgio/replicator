package ru.splite.replicator.atlas.state

import kotlinx.serialization.Serializable
import ru.splite.replicator.atlas.AtlasMessage
import ru.splite.replicator.transport.NodeIdentifier

@Serializable
data class CommandState(
    val status: CommandStatus = CommandStatus.START,
    val quorum: Set<NodeIdentifier> = emptySet(),
    val command: Command = Command.WithNoop,
    val ballot: Long = 0,
    val acceptedBallot: Long = 0,
    val consensusValue: AtlasMessage.ConsensusValue? = null
)
