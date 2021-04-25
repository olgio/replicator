package ru.splite.replicator.atlas.state

import ru.splite.replicator.atlas.id.Id
import ru.splite.replicator.transport.NodeIdentifier

interface CommandStateStore {

    fun getCommandState(commandId: Id<NodeIdentifier>): CommandState?

    fun setCommandState(commandId: Id<NodeIdentifier>, commandState: CommandState): CommandState
}