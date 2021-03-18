package ru.splite.replicator.cluster

import kotlinx.coroutines.*
import ru.splite.replicator.AtlasCommandSubmitter
import ru.splite.replicator.AtlasProtocolConfig
import ru.splite.replicator.AtlasProtocolController
import ru.splite.replicator.BaseAtlasProtocol
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.executor.CommandExecutor
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.graph.JGraphTDependencyGraph
import ru.splite.replicator.id.InMemoryIdGenerator
import ru.splite.replicator.keyvalue.KeyValueStateMachine
import ru.splite.replicator.transport.CoroutineChannelTransport
import ru.splite.replicator.transport.sender.MessageSender

class AtlasClusterBuilder {

    class AtlasClusterScope(
        val transport: CoroutineChannelTransport,
        private val coroutineScope: CoroutineScope,
        private val jobs: MutableList<Job> = mutableListOf()
    ) {

        internal fun buildNode(i: Int, config: AtlasProtocolConfig): AtlasClusterNode {
            val nodeIdentifier = NodeIdentifier("node-$i")
            val stateMachine = KeyValueStateMachine()
            val dependencyGraph = JGraphTDependencyGraph<Dependency>()
            val commandExecutor = CommandExecutor(dependencyGraph, stateMachine)
            val idGenerator = InMemoryIdGenerator(nodeIdentifier)
            val atlasProtocol = BaseAtlasProtocol(
                nodeIdentifier,
                config,
                i.toLong(),
                idGenerator,
                stateMachine.newConflictIndex(),
                commandExecutor
            )
            val atlasProtocolController = AtlasProtocolController(transport, atlasProtocol)
            val messageSender = MessageSender(atlasProtocolController, atlasProtocol.config.sendMessageTimeout)
            val atlasCommandSubmitter =
                AtlasCommandSubmitter(atlasProtocol, messageSender, coroutineScope, commandExecutor)

            val commandExecutorCoroutineName = SupervisorJob()
                .plus(CoroutineName("${nodeIdentifier.identifier}|ce"))
            jobs.add(commandExecutor.launchCommandExecutor(commandExecutorCoroutineName, coroutineScope))
            return AtlasClusterNode(nodeIdentifier, atlasCommandSubmitter, stateMachine)
        }

        suspend fun awaitTermination() {
            val childrenJobs = coroutineScope.coroutineContext.job.children
            childrenJobs.minus(jobs).filter { it is CompletableJob }.forEach {
                it.join()
            }
            jobs.forEach { it.cancel() }
        }
    }

    suspend fun buildNodes(
        coroutineScope: CoroutineScope,
        config: AtlasProtocolConfig,
        action: suspend AtlasClusterScope.(List<AtlasClusterNode>) -> Unit
    ) {
        val transport = CoroutineChannelTransport(coroutineScope)
        val scope = AtlasClusterScope(transport, coroutineScope)
        val nodes = (1..config.n).map {
            scope.buildNode(it, config)
        }
        try {
            action.invoke(scope, nodes)
        } finally {
            scope.awaitTermination()
        }
    }

    suspend fun buildNodes(
        coroutineScope: CoroutineScope,
        n: Int,
        f: Int,
        action: suspend AtlasClusterScope.(List<AtlasClusterNode>) -> Unit
    ) {
        val config = AtlasProtocolConfig(n = n, f = f)
        return buildNodes(coroutineScope, config, action)
    }
}