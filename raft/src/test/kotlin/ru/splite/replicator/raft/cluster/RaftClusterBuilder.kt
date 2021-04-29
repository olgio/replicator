package ru.splite.replicator.raft.cluster

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Job
import kotlinx.coroutines.test.TestCoroutineScope
import ru.splite.replicator.demo.keyvalue.KeyValueStateMachine
import ru.splite.replicator.log.InMemoryReplicatedLogStore
import ru.splite.replicator.raft.JobLauncher
import ru.splite.replicator.raft.RaftCommandSubmitter
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.raft.RaftProtocolController
import ru.splite.replicator.raft.protocol.BaseRaftProtocol
import ru.splite.replicator.raft.state.InMemoryNodeStateStore
import ru.splite.replicator.timer.flow.DelayTimerFactory
import ru.splite.replicator.timer.flow.TimeTick
import ru.splite.replicator.transport.CoroutineChannelTransport
import ru.splite.replicator.transport.NodeIdentifier
import kotlin.random.Random

class RaftClusterBuilder {

    class RaftClusterScope(
        val transport: CoroutineChannelTransport,
        private val coroutineScope: TestCoroutineScope,
        internal val jobs: MutableList<Job> = mutableListOf()
    ) {

        internal fun buildNode(name: String, fullSize: Int): RaftClusterNode {
            val timerFactory = DelayTimerFactory(
                random = Random(0L),
                currentTimeTick = { TimeTick(coroutineScope.currentTime) })

            val nodeIdentifier = NodeIdentifier(name)
            val logStore = InMemoryReplicatedLogStore()
            val stateMachine = KeyValueStateMachine()
            val nodeStateStore = InMemoryNodeStateStore()
            val config = RaftProtocolConfig(
                address = nodeIdentifier,
                processId = 0L,
                n = fullSize
            )

            val raftProtocol = BaseRaftProtocol(logStore, config, nodeStateStore)

            val raftProtocolController = RaftProtocolController(transport, config, raftProtocol)

            val jobLauncher = JobLauncher(raftProtocolController, timerFactory)
            jobs.add(
                jobLauncher.launchTermClock(
                    coroutineContext = CoroutineName("$nodeIdentifier|term-clock"),
                    coroutineScope = coroutineScope,
                    period = 3000L..4000L
                )
            )
            jobs.add(
                jobLauncher.launchAppendEntriesSender(
                    coroutineContext = CoroutineName("${nodeIdentifier}|append-entries-sender"),
                    coroutineScope = coroutineScope,
                    period = 1000L..1000L
                )
            )

            val raftCommandSubmitter = RaftCommandSubmitter(raftProtocolController, stateMachine)
            jobs.add(
                raftCommandSubmitter.launchCommandApplier(
                    coroutineContext = CoroutineName("${nodeIdentifier}|command-applier"),
                    coroutineScope = coroutineScope
                )
            )

            return RaftClusterNode(
                address = nodeIdentifier,
                protocol = raftProtocol,
                commandSubmitter = raftCommandSubmitter,
                logStore = logStore,
                stateMachine = stateMachine
            )
        }
    }

    suspend fun buildNodes(
        coroutineScope: TestCoroutineScope,
        n: Int,
        action: suspend RaftClusterScope.(List<RaftClusterNode>) -> Unit
    ) {
        val transport = CoroutineChannelTransport(coroutineScope)
        val scope = RaftClusterScope(transport, coroutineScope)
        val nodes = (1..n).map {
            scope.buildNode("node-$it", n)
        }
        try {
            action.invoke(scope, nodes)
        } finally {
            scope.jobs.forEach { it.cancel() }
        }
    }
}