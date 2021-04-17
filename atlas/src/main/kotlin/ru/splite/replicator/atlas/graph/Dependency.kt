package ru.splite.replicator.atlas.graph

import kotlinx.serialization.Serializable
import ru.splite.replicator.atlas.id.Id
import ru.splite.replicator.transport.NodeIdentifier

@Serializable
data class Dependency(val dot: Id<NodeIdentifier>) : Comparable<Dependency> {

    override fun compareTo(other: Dependency): Int {
        val nodeIdCompare = this.dot.node.identifier.compareTo(other.dot.node.identifier)
        return if (nodeIdCompare != 0) {
            nodeIdCompare
        } else {
            this.dot.id.compareTo(other.dot.id)
        }
    }
}
