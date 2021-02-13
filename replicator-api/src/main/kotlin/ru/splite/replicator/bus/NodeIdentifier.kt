package ru.splite.replicator.bus

import kotlinx.serialization.Serializable

@Serializable
data class NodeIdentifier(val identifier: String) {

    override fun toString(): String {
        return identifier
    }

    companion object {
        fun fromInt(value: Int): NodeIdentifier {
            return NodeIdentifier(value.toString())
        }
    }
}
