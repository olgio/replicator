package ru.splite.replicator.atlas.rocksdb

import ru.splite.replicator.atlas.id.Id
import ru.splite.replicator.atlas.state.CommandState
import ru.splite.replicator.atlas.state.CommandStateStore
import ru.splite.replicator.rocksdb.RocksDbStore
import ru.splite.replicator.transport.NodeIdentifier

class RocksDbCommandStateStore(db: RocksDbStore) : CommandStateStore {

    private val commandStateStore = db.createColumnFamilyStore(COMMAND_STATE_COLUMN_FAMILY_NAME)

    override fun getCommandState(commandId: Id<NodeIdentifier>): CommandState? =
        commandStateStore.getAsType(
            commandId,
            Id.serializer(NodeIdentifier.serializer()),
            CommandState.serializer()
        )

    override fun setCommandState(commandId: Id<NodeIdentifier>, commandState: CommandState): CommandState =
        commandState.apply {
            commandStateStore.putAsType(
                commandId,
                commandState,
                Id.serializer(NodeIdentifier.serializer()),
                CommandState.serializer()
            )
        }

    companion object {
        private const val COMMAND_STATE_COLUMN_FAMILY_NAME = "COMMAND_STATE"

        val COLUMN_FAMILY_NAMES = listOf(
            COMMAND_STATE_COLUMN_FAMILY_NAME
        )
    }
}