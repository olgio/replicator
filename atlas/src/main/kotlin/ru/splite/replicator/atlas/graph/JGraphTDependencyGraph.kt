package ru.splite.replicator.atlas.graph

import org.jgrapht.alg.connectivity.KosarajuStrongConnectivityInspector
import org.jgrapht.graph.AsSubgraph
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.EdgeReversedGraph
import org.jgrapht.graph.SimpleDirectedGraph
import org.jgrapht.traverse.BreadthFirstIterator
import org.jgrapht.traverse.TopologicalOrderIterator

class JGraphTDependencyGraph<K : Comparable<K>>(
    private val dependencyGraphStore: DependencyGraphStore<K> = EmptyDependencyGraphStore()
) : DependencyGraph<K> {

    override val numVertices: Int
        get() = graph.vertexSet().size

    private val graph = SimpleDirectedGraph<K, DefaultEdge>(DefaultEdge::class.java).apply {
        dependencyGraphStore.getDependencies().forEach { (key, dependencies) ->
            addVertex(key)

            dependencies.forEach {
                addVertex(it)
                addEdge(key, it)
            }
        }
    }

    private val committed = dependencyGraphStore.getStatuses().filter { (_, status) ->
        status == DependencyStatus.COMMITTED
    }.map { it.first }.toMutableSet()

    //private val sequenceNumbers = mutableMapOf<K,>()

    private val executed = dependencyGraphStore.getStatuses().filter { (_, status) ->
        status == DependencyStatus.APPLIED
    }.map { it.first }.toMutableSet()

    override fun commit(key: K, dependencies: Set<K>) {
        if (committed.contains(key) || executed.contains(key)) {
            return
        }

        dependencyGraphStore.setDependenciesPerKey(key, dependencies)
        dependencyGraphStore.setStatusPerKey(key, DependencyStatus.COMMITTED)

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
            dependencyGraphStore.setStatusPerKey(key, DependencyStatus.APPLIED)
            dependencyGraphStore.deleteDependenciesPerKey(key)

            graph.removeVertex(key)
            committed.remove(key)
            //sequenceNumbers -= key
            executed.add(key)
        }

        return KeysToExecute(executable, graph.vertexSet().filter { !committed.contains(it) }.toList())
    }

}