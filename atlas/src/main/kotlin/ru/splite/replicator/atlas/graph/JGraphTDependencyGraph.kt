package ru.splite.replicator.atlas.graph

import kotlinx.coroutines.runBlocking
import org.jgrapht.Graph
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

    private val commandStatuses = runBlocking {
        ConcurrentHashMap(
            dependencyGraphStore
                .getStatuses()
                .map {
                    it.first to it.second
                }.toMap()
        )
    }

    private val graph = runBlocking {
        SimpleDirectedGraph<K, DefaultEdge>(DefaultEdge::class.java).apply {
            dependencyGraphStore.getDependencies().forEach { (key, dependencies) ->
                addDependenciesToGraph(key, dependencies)
            }
        }
    }

    override suspend fun commit(key: K, dependencies: Set<K>) {
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

    private fun isEligible(key: K): Boolean {
        val iterator = BreadthFirstIterator(graph, key)
        return commandStatuses[key] == DependencyStatus.COMMITTED &&
                iterator.asSequence().all { commandStatuses[it] == DependencyStatus.COMMITTED }
    }

    override suspend fun evaluateKeyToExecute(): KeysToExecute<K> {
        var committedCommandsCount = 0
        while (committedQueue.isNotEmpty()) {
            val keyWithDependencies = committedQueue.poll() ?: break
            commandStatuses[keyWithDependencies.key] = DependencyStatus.COMMITTED
            graph.addDependenciesToGraph(keyWithDependencies.key, keyWithDependencies.dependencies)
            committedCommandsCount++
        }
        LOGGER.debug("Added {} committed commands to graph", committedCommandsCount)

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

    private fun Graph<K, DefaultEdge>.addDependenciesToGraph(key: K, dependencies: Set<K>) {
        this.addVertex(key)
        dependencies.filter { commandStatuses[it] != DependencyStatus.APPLIED }.forEach {
            this.addVertex(it)
            this.addEdge(key, it)
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}