package ru.splite.replicator.raft.message

import ru.splite.replicator.bus.NodeIdentifier

class StubClusterTopology<C> : ClusterTopology<C> {

    private val nodeIdentifierToReceiver: MutableMap<NodeIdentifier, RaftMessageReceiver<C>> = mutableMapOf()

    override val nodes: Collection<NodeIdentifier>
        get() = nodeIdentifierToReceiver.keys

    override fun get(nodeIdentifier: NodeIdentifier): RaftMessageReceiver<C> {
        return nodeIdentifierToReceiver[nodeIdentifier] ?: error("No node $nodeIdentifier in cluster")
    }

    operator fun set(nodeIdentifier: NodeIdentifier, receiver: RaftMessageReceiver<C>) {
        if (nodeIdentifierToReceiver.containsKey(nodeIdentifier)) {
            error("Node $nodeIdentifier already exists in cluster")
        }
        nodeIdentifierToReceiver[nodeIdentifier] = receiver
    }
}