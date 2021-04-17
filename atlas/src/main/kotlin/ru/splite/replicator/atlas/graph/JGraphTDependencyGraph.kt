package ru.splite.replicator.atlas.graph

import org.jgrapht.alg.connectivity.KosarajuStrongConnectivityInspector
import org.jgrapht.graph.AsSubgraph
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.EdgeReversedGraph
import org.jgrapht.graph.SimpleDirectedGraph
import org.jgrapht.traverse.BreadthFirstIterator
import org.jgrapht.traverse.TopologicalOrderIterator

class JGraphTDependencyGraph<K : Comparable<K>> : DependencyGraph<K> {

    override val numVertices: Int
        get() = graph.vertexSet().size

    private val graph = SimpleDirectedGraph<K, DefaultEdge>(DefaultEdge::class.java)

    private val committed = mutableSetOf<K>()

    //private val sequenceNumbers = mutableMapOf<K,>()

    private val executed = mutableSetOf<K>()

    override fun commit(key: K, dependencies: Set<K>) {
        if (committed.contains(key) || executed.contains(key)) {
            return
        }

        committed.add(key)
        //sequenceNumbers(key) = sequenceNumber

        graph.addVertex(key)

        dependencies.minus(executed).forEach {
            graph.addVertex(it)
            graph.addEdge(key, it)
        }
    }

    fun updateExecuted(keys: Set<K>) {
        executed.addAll(keys)
        committed.removeAll(keys)
        //sequenceNumbers.retain({ case (key, _) => !executed.contains(key) })

        val verticesToRemove = mutableSetOf<K>()
        graph.vertexSet().forEach { key ->
            if (executed.contains(key)) {
                verticesToRemove += key
            }
        }
        graph.removeAllVertices(verticesToRemove)
    }

    private fun isEligible(key: K): Boolean {
        val iterator = BreadthFirstIterator(graph, key)
        return committed.contains(key) &&
                iterator.asSequence().all { committed.contains(it) }
    }

    override fun evaluateKeyToExecute(): KeysToExecute<K> {
        val eligible = graph.vertexSet().filter { isEligible(it) }.toSet()

        val eligibleGraph = AsSubgraph(graph, eligible)
        val components = KosarajuStrongConnectivityInspector(eligibleGraph)
        val condensation = components.condensation
        val reversed = EdgeReversedGraph(condensation)
        val iterator = TopologicalOrderIterator(reversed)
        val executable = iterator.asSequence().map { component ->
            component.vertexSet().sortedBy { it }
        }.flatten().toList()

        executable.forEach { key ->
            graph.removeVertex(key)
            committed.remove(key)
            //sequenceNumbers -= key
            executed.add(key)
        }

        return KeysToExecute(executable, graph.vertexSet().filter { !committed.contains(it) }.toList())
    }

}