package ru.splite.replicator.atlas.state

import ru.splite.replicator.atlas.id.Id
import ru.splite.replicator.transport.NodeIdentifier
import java.util.concurrent.ConcurrentHashMap

class InMemoryCommandStateStore : CommandStateStore {

    private val commands = ConcurrentHashMap<Id<NodeIdentifier>, CommandState>()

    override suspend fun getCommandState(commandId: Id<NodeIdentifier>): CommandState? =
        commands[commandId]

    override suspend fun setCommandState(commandId: Id<NodeIdentifier>, commandState: CommandState): CommandState =
        commandState.apply {
            commands[commandId] = this
        }
}