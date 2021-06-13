package ru.splite.replicator.raft.state

import ru.splite.replicator.transport.NodeIdentifier
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicReference

class InMemoryNodeStateStore : NodeStateStore {

    private val currentState = AtomicReference(LocalNodeState())

    private val externalNodeStates = ConcurrentHashMap<NodeIdentifier, ExternalNodeState>()

    override fun getState(): LocalNodeState = currentState.get()

    override suspend fun setState(localNodeState: LocalNodeState): LocalNodeState {
        val oldState = currentState.get()
        check(oldState.currentTerm <= localNodeState.currentTerm) {
            "Cannot decrease term: ${oldState.currentTerm} > ${localNodeState.currentTerm}"
        }
        val isUpdated = currentState.compareAndSet(oldState, localNodeState)
        check(isUpdated) {
            "State updated concurrently"
        }
        return localNodeState
    }

    override fun getExternalNodeState(nodeIdentifier: NodeIdentifier): ExternalNodeState =
        externalNodeStates[nodeIdentifier] ?: error("Cannot resolve externalNodeState for $nodeIdentifier")

    override suspend fun setExternalNodeState(
        nodeIdentifier: NodeIdentifier,
        externalNodeState: ExternalNodeState
    ): ExternalNodeState {
        externalNodeStates[nodeIdentifier] = externalNodeState
        return externalNodeState
    }
}