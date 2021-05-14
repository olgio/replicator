package ru.splite.replicator.atlas.graph

import org.jgrapht.alg.connectivity.KosarajuStrongConnectivityInspector
import org.jgrapht.graph.AsSubgraph
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.EdgeReversedGraph
import org.jgrapht.graph.SimpleDirectedGraph
import org.jgrapht.traverse.BreadthFirstIterator
import org.jgrapht.traverse.TopologicalOrderIterator
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue

class JGraphTDependencyGraph<K : Comparable<K>>(
    private val dependencyGraphStore: DependencyGraphStore<K> = EmptyDependencyGraphStore()
) : DependencyGraph<K> {

    private data class KeyWithDependencies<K>(val key: K, val dependencies: Set<K>)

    override val numVertices: Int
        get() = graph.vertexSet().size

    private val committedQueue = ConcurrentLinkedQueue<KeyWithDependencies<K>>()

    private val commandStatuses = ConcurrentHashMap(
        dependencyGraphStore
            .getStatuses()
            .map {
                it.first to it.second
            }.toMap()
    )

    private val graph = SimpleDirectedGraph<K, DefaultEdge>(DefaultEdge::class.java).apply {
        dependencyGraphStore.getDependencies().forEach { (key, dependencies) ->
            addDependenciesToGraph(key, dependencies)
        }
    }

    override fun commit(key: K, dependencies: Set<K>) {
        val currentStatus = commandStatuses[key]
        if (currentStatus == DependencyStatus.COMMITTED
            || currentStatus == DependencyStatus.APPLIED
        ) {
            return
        }

        dependencyGraphStore.setDependenciesPerKey(key, dependencies)
        dependencyGraphStore.setStatusPerKey(key, DependencyStatus.COMMITTED)

        committedQueue.add(KeyWithDependencies(key, dependencies))
    }

//    fun updateExecuted(keys: Set<K>) {
//        executed.addAll(keys)
//        committed.removeAll(keys)
//        //sequenceNumbers.retain({ case (key, _) => !executed.contains(key) })
//
//        val verticesToRemove = mutableSetOf<K>()
//        graph.vertexSet().forEach { key ->
//            if (executed.contains(key)) {
//                verticesToRemove += key
//            }
//        }
//        graph.removeAllVertices(verticesToRemove)
//    }

    private fun isEligible(key: K): Boolean {
        val iterator = BreadthFirstIterator(graph, key)
        return commandStatuses[key] == DependencyStatus.COMMITTED &&
                iterator.asSequence().all { commandStatuses[it] == DependencyStatus.COMMITTED }
    }

    override fun evaluateKeyToExecute(): KeysToExecute<K> {
        for (i in (0 until MAX_COMMITTED_BATCH_SIZE)) {
            val keyWithDependencies = committedQueue.poll()
            if (keyWithDependencies == null) {
                LOGGER.debug("Added {} committed commands to graph", i)
                break
            }
            commandStatuses[keyWithDependencies.key] = DependencyStatus.COMMITTED
            addDependenciesToGraph(keyWithDependencies.key, keyWithDependencies.dependencies)
        }

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
            commandStatuses[key] = DependencyStatus.APPLIED
        }

        return KeysToExecute(
            executable,
            graph.vertexSet().filter { commandStatuses[it] != DependencyStatus.COMMITTED }.toList()
        )
    }

    private fun addDependenciesToGraph(key: K, dependencies: Set<K>) {
        graph.addVertex(key)
        dependencies.filter { commandStatuses[it] != DependencyStatus.APPLIED }.forEach {
            graph.addVertex(it)
            graph.addEdge(key, it)
        }
    }

    companion object {
        private const val MAX_COMMITTED_BATCH_SIZE = 256

        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}