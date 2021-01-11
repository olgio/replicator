package ru.splite.replicator.bus

data class NodeIdentifier(val identifier: String) {

    override fun toString(): String {
        return identifier
    }
}
