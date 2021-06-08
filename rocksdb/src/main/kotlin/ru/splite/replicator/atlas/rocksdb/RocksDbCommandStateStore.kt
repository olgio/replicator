package ru.splite.replicator.atlas.rocksdb

import ru.splite.replicator.atlas.id.Id
import ru.splite.replicator.atlas.state.CommandState
import ru.splite.replicator.atlas.state.CommandStateStore
import ru.splite.replicator.atlas.state.CommandStatus
import ru.splite.replicator.rocksdb.RocksDbStore
import ru.splite.replicator.transport.NodeIdentifier
import java.util.concurrent.ConcurrentHashMap

class RocksDbCommandStateStore(db: RocksDbStore) : CommandStateStore {

    private val commandStateStore = db.createColumnFamilyStore(COMMAND_STATE_COLUMN_FAMILY_NAME)

    private val commandStateCache = ConcurrentHashMap<Id<NodeIdentifier>, CommandState>()

    override suspend fun getCommandState(commandId: Id<NodeIdentifier>): CommandState? =
        commandStateCache[commandId] ?: commandStateStore.getAsType(
            commandId,
            Id.serializer(NodeIdentifier.serializer()),
            CommandState.serializer()
        )

    override suspend fun setCommandState(commandId: Id<NodeIdentifier>, commandState: CommandState): CommandState {
        commandStateStore.putAsType(
            commandId,
            commandState,
            Id.serializer(NodeIdentifier.serializer()),
            CommandState.serializer()
        )
        if (commandState.status == CommandStatus.COMMIT) {
            commandStateCache.remove(commandId)
        } else {
            commandStateCache[commandId] = commandState
        }
        return commandState
    }

    companion object {
        private const val COMMAND_STATE_COLUMN_FAMILY_NAME = "COMMAND_STATE"

        val COLUMN_FAMILY_NAMES = listOf(
            COMMAND_STATE_COLUMN_FAMILY_NAME
        )
    }
}