package ru.splite.replicator.raft.message

import ru.splite.replicator.bus.NodeIdentifier

interface ClusterTopology<C> {

    val nodes: Collection<NodeIdentifier>

    operator fun get(nodeIdentifier: NodeIdentifier): RaftMessageReceiver<C>
}