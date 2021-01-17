package ru.splite.replicator.raft.message

import ru.splite.replicator.bus.NodeIdentifier

interface ClusterTopology<out R> {

    val nodes: Collection<NodeIdentifier>

    operator fun get(nodeIdentifier: NodeIdentifier): R
}