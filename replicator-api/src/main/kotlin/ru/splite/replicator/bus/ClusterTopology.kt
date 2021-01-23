package ru.splite.replicator.bus

interface ClusterTopology<out R> {

    val nodes: Collection<NodeIdentifier>

    operator fun get(nodeIdentifier: NodeIdentifier): R
}