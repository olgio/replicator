package ru.splite.replicator.raft.message

import ru.splite.replicator.bus.NodeIdentifier

class StubClusterTopology<R> : ClusterTopology<R> {

    private val nodeIdentifierToReceiver: MutableMap<NodeIdentifier, R> = mutableMapOf()

    override val nodes: Collection<NodeIdentifier>
        get() = nodeIdentifierToReceiver.keys

    override fun get(nodeIdentifier: NodeIdentifier): R {
        return nodeIdentifierToReceiver[nodeIdentifier] ?: error("No node $nodeIdentifier in cluster")
    }

    operator fun set(nodeIdentifier: NodeIdentifier, receiver: R) {
        if (nodeIdentifierToReceiver.containsKey(nodeIdentifier)) {
            error("Node $nodeIdentifier already exists in cluster")
        }
        nodeIdentifierToReceiver[nodeIdentifier] = receiver
    }
}