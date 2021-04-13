package ru.splite.replicator.state

import ru.splite.replicator.transport.NodeIdentifier

class CommandState(
    var status: Status = Status.START,
    var quorum: Set<NodeIdentifier> = emptySet(),
    var command: Command = Command.WithNoop
) {

    val synodState by lazy {
        SynodState()
    }

    enum class Status { START, COLLECT, PAYLOAD, COMMIT, RECOVERY }
}
