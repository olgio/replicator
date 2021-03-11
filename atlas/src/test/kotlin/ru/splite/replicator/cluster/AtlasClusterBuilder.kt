package ru.splite.replicator.cluster

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Job
import kotlinx.coroutines.test.TestCoroutineScope
import ru.splite.replicator.AtlasCommandSubmitter
import ru.splite.replicator.AtlasProtocolConfig
import ru.splite.replicator.AtlasProtocolController
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.executor.CommandExecutor
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.graph.JGraphTDependencyGraph
import ru.splite.replicator.id.InMemoryIdGenerator
import ru.splite.replicator.keyvalue.KeyValueStateMachine
import ru.splite.replicator.transport.CoroutineChannelTransport

class AtlasClusterBuilder() {

    class AtlasClusterScope(
        val transport: CoroutineChannelTransport,
        private val coroutineScope: TestCoroutineScope,
        internal val jobs: MutableList<Job> = mutableListOf()
    ) {

        internal fun buildNode(i: Int, n: Int, f: Int): AtlasClusterNode {
            val nodeIdentifier = NodeIdentifier("node-$i")
            val stateMachine = KeyValueStateMachine()
            val dependencyGraph = JGraphTDependencyGraph<Dependency>()
            val commandExecutor = CommandExecutor(dependencyGraph, stateMachine)
            val config = AtlasProtocolConfig(n = n, f = f)
            val idGenerator = InMemoryIdGenerator(nodeIdentifier)
            val atlasProtocol = AtlasProtocolController(
                nodeIdentifier,
                transport,
                i.toLong(),
                idGenerator,
                stateMachine.newConflictIndex(),
                commandExecutor,
                config
            )
            val atlasCommandSubmitter = AtlasCommandSubmitter(atlasProtocol, commandExecutor)

            val commandExecutorCoroutineName = CoroutineName("${nodeIdentifier.identifier}|command-executor")
            jobs.add(commandExecutor.launchCommandExecutor(commandExecutorCoroutineName, coroutineScope))
            return AtlasClusterNode(nodeIdentifier, atlasCommandSubmitter, stateMachine)
        }
    }

    suspend fun buildNodes(
        coroutineScope: TestCoroutineScope,
        n: Int,
        f: Int,
        action: suspend AtlasClusterScope.(List<AtlasClusterNode>) -> Unit
    ) {
        val transport = CoroutineChannelTransport(coroutineScope)
        val scope = AtlasClusterScope(transport, coroutineScope)
        val nodes = (1..n).map {
            scope.buildNode(it, n, f)
        }
        try {
            action.invoke(scope, nodes)
        } finally {
            scope.jobs.forEach { it.cancel() }
        }
    }
}