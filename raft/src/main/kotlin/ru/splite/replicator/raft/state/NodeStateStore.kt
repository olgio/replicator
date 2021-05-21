package ru.splite.replicator.raft.state

import ru.splite.replicator.transport.NodeIdentifier

interface NodeStateStore {

    fun getState(): LocalNodeState

    suspend fun setState(localNodeState: LocalNodeState): LocalNodeState

    fun getExternalNodeState(nodeIdentifier: NodeIdentifier): ExternalNodeState

    suspend fun setExternalNodeState(
        nodeIdentifier: NodeIdentifier,
        externalNodeState: ExternalNodeState
    ): ExternalNodeState
}