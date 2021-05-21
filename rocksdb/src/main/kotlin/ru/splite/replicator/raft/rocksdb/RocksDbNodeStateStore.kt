package ru.splite.replicator.raft.rocksdb

import kotlinx.coroutines.runBlocking
import ru.splite.replicator.raft.state.ExternalNodeState
import ru.splite.replicator.raft.state.LocalNodeState
import ru.splite.replicator.raft.state.NodeStateStore
import ru.splite.replicator.rocksdb.RocksDbStore
import ru.splite.replicator.transport.NodeIdentifier
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

class RocksDbNodeStateStore(db: RocksDbStore) : NodeStateStore {

    private val localStateStore = db.createColumnFamilyStore(STATE_COLUMN_FAMILY_NAME)

    private val externalStateStore = db.createColumnFamilyStore(EXTERNAL_COLUMN_FAMILY_NAME)

    private val currentState = runBlocking {
        AtomicReference(readState())
    }

    private val externalNodeStates = runBlocking {
        ConcurrentHashMap(readExternalStates())
    }

    override fun getState(): LocalNodeState {
        return currentState.get()
    }

    override suspend fun setState(localNodeState: LocalNodeState): LocalNodeState {
        localStateStore.putAsType(STATE_KEY, localNodeState, LocalNodeState.serializer())
        currentState.set(localNodeState)
        return localNodeState
    }

    override fun getExternalNodeState(nodeIdentifier: NodeIdentifier): ExternalNodeState {
        return externalNodeStates[nodeIdentifier]!!
    }

    override suspend fun setExternalNodeState(
        nodeIdentifier: NodeIdentifier,
        externalNodeState: ExternalNodeState
    ): ExternalNodeState {
        externalStateStore.putAsType(nodeIdentifier.identifier, externalNodeState, ExternalNodeState.serializer())
        externalNodeStates[nodeIdentifier] = externalNodeState
        return externalNodeState
    }

    private suspend fun readState(): LocalNodeState {
        return localStateStore.getAsType(STATE_KEY, LocalNodeState.serializer()) ?: LocalNodeState()
    }

    private suspend fun readExternalStates(): Map<NodeIdentifier, ExternalNodeState> {
        return externalStateStore
            .getAll(ExternalNodeState.serializer())
            .map { NodeIdentifier(it.key) to it.value }
            .toMap()
    }

    companion object {
        private const val STATE_KEY = "STATE"
        private const val STATE_COLUMN_FAMILY_NAME = "NODE_STATE"
        private const val EXTERNAL_COLUMN_FAMILY_NAME = "EXTERNAL_STATE"
        val COLUMN_FAMILY_NAMES = listOf(STATE_COLUMN_FAMILY_NAME, EXTERNAL_COLUMN_FAMILY_NAME)
    }
}