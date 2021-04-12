package ru.splite.replicator.cluster

import kotlinx.coroutines.*
import ru.splite.replicator.AtlasCommandSubmitter
import ru.splite.replicator.AtlasProtocolConfig
import ru.splite.replicator.AtlasProtocolController
import ru.splite.replicator.BaseAtlasProtocol
import ru.splite.replicator.demo.keyvalue.KeyValueStateMachine
import ru.splite.replicator.executor.CommandExecutor
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.graph.JGraphTDependencyGraph
import ru.splite.replicator.id.InMemoryIdGenerator
import ru.splite.replicator.timer.flow.DelayTimerFactory
import ru.splite.replicator.transport.CoroutineChannelTransport
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.sender.MessageSender

class AtlasClusterBuilder {

    class AtlasClusterScope(
        val transport: CoroutineChannelTransport,
        private val coroutineScope: CoroutineScope,
        private val jobs: MutableList<Job> = mutableListOf()
    ) {

        internal fun buildNode(config: AtlasProtocolConfig): AtlasClusterNode {
            val timerFactory = DelayTimerFactory()
            val stateMachine = KeyValueStateMachine()
            val dependencyGraph = JGraphTDependencyGraph<Dependency>()
            val commandExecutor = CommandExecutor(dependencyGraph, stateMachine)
            val idGenerator = InMemoryIdGenerator(config.address)
            val atlasProtocol = BaseAtlasProtocol(
                config,
                idGenerator,
                stateMachine.newConflictIndex(),
                commandExecutor
            )
            val atlasProtocolController = AtlasProtocolController(transport, atlasProtocol)
            val messageSender = MessageSender(atlasProtocolController, atlasProtocol.config.sendMessageTimeout)
            val atlasCommandSubmitter =
                AtlasCommandSubmitter(atlasProtocol, messageSender, coroutineScope, commandExecutor)
            val commandRecoveryCoroutineContext = SupervisorJob()
                .plus(CoroutineName("command-recovery"))
            jobs.add(
                atlasCommandSubmitter.launchCommandRecoveryLoop(
                    commandRecoveryCoroutineContext,
                    coroutineScope, timerFactory
                )
            )

            val commandExecutorCoroutineContext = SupervisorJob()
                .plus(CoroutineName("command-executor"))
            jobs.add(
                commandExecutor.launchCommandExecutor(
                    commandExecutorCoroutineContext,
                    coroutineScope
                )
            )
            return AtlasClusterNode(config.address, atlasCommandSubmitter, stateMachine)
        }

        suspend fun awaitTermination() {
            val childrenJobs = coroutineScope.coroutineContext.job.children
            childrenJobs.minus(jobs).filter { it is CompletableJob }.forEach {
                it.join()
            }
        }

        fun cancelJobs() {
            jobs.forEach { it.cancel() }
        }
    }

    suspend fun buildNodes(
        coroutineScope: CoroutineScope,
        n: Int,
        buildConfigAction: (Int) -> AtlasProtocolConfig,
        action: suspend AtlasClusterScope.(List<AtlasClusterNode>) -> Unit
    ) {
        val transport = CoroutineChannelTransport(coroutineScope)
        val scope = AtlasClusterScope(transport, coroutineScope)
        val nodes = (1..n).map {
            scope.buildNode(buildConfigAction(it))
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
        val configBuilder = { i: Int ->
            val nodeIdentifier = NodeIdentifier("node-$i")

            AtlasProtocolConfig(
                address = nodeIdentifier,
                processId = i.toLong(),
                n = n,
                f = f
            )
        }
        return buildNodes(coroutineScope, n, configBuilder, action)
    }
}