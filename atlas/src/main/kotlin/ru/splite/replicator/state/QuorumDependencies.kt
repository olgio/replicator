package ru.splite.replicator.state

import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.transport.NodeIdentifier

class QuorumDependencies {

    private val participants: MutableSet<NodeIdentifier> = mutableSetOf()

    private val thresholdDependencies: MutableMap<Dependency, Int> = mutableMapOf()

    val dependenciesUnion: Set<Dependency>
        get() = thresholdDependencies.keys

    fun addParticipant(nodeIdentifier: NodeIdentifier, deps: Set<Dependency>, quorumSize: Int) {
        if (this.participants.size >= quorumSize) {
            error("Fast quorum invariant violated")
        }

        val isAdded = this.participants.add(nodeIdentifier)

        if (!isAdded) {
            error("Dependencies duplication detected for node $nodeIdentifier")
        }

        deps.forEach {
            this.thresholdDependencies.merge(it, 1) { oldValue, newValue ->
                oldValue + newValue
            }
        }
    }

    fun isQuorumCompleted(quorumSize: Int): Boolean {
        return quorumSize == this.participants.size
    }

    fun checkThresholdUnion(threshold: Int): Boolean {
        return this.thresholdDependencies.all { it.value >= threshold }
    }

    fun checkUnion(): Boolean {
        return false //TODO
    }
}