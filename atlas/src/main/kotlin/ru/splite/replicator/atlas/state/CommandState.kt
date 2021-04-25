package ru.splite.replicator.atlas.state

import ru.splite.replicator.transport.NodeIdentifier

class CommandState(
    var status: CommandStatus = CommandStatus.START,
    var quorum: Set<NodeIdentifier> = emptySet(),
    var command: Command = Command.WithNoop
) {

    val synodState by lazy {
        SynodState()
    }

}
