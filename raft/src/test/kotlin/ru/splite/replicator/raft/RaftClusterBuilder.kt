package ru.splite.replicator.raft

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.Job
import kotlinx.coroutines.test.TestCoroutineScope
import ru.splite.replicator.keyvalue.KeyValueStateMachine
import ru.splite.replicator.log.InMemoryReplicatedLogStore
import ru.splite.replicator.raft.state.RaftLocalNodeState
import ru.splite.replicator.timer.flow.DelayTimerFactory
import ru.splite.replicator.timer.flow.TimeTick
import ru.splite.replicator.transport.CoroutineChannelTransport
import ru.splite.replicator.transport.NodeIdentifier

class RaftClusterBuilder {

    class RaftClusterScope(
        val transport: CoroutineChannelTransport,
        private val coroutineScope: TestCoroutineScope,
        internal val jobs: MutableList<Job> = mutableListOf()
    ) {

        internal fun buildNode(name: String, fullSize: Int): RaftCommandSubmitter {
            val timerFactory = DelayTimerFactory(currentTimeTick = { TimeTick(coroutineScope.currentTime) })

            val nodeIdentifier = NodeIdentifier(name)
            val logStore = InMemoryReplicatedLogStore()
            val localNodeState = RaftLocalNodeState(nodeIdentifier)
            val stateMachine = KeyValueStateMachine()
            val config = RaftProtocolConfig(n = fullSize)

            val raftProtocol = RaftProtocolController(
                logStore,
                transport,
                config,
                localNodeState
            )

            val termClockScheduler = TermClockScheduler(raftProtocol, timerFactory)
            jobs.add(
                termClockScheduler.launchTermClock(
                    coroutineContext = CoroutineName("$nodeIdentifier|term-clock"),
                    coroutineScope = coroutineScope,
                    period = 3000L..4000L
                )
            )

            val raftCommandSubmitter = RaftCommandSubmitter(raftProtocol, stateMachine, timerFactory)
            jobs.add(
                raftCommandSubmitter.launchCommandApplier(
                    coroutineContext = CoroutineName("${nodeIdentifier}|command-applier"),
                    coroutineScope = coroutineScope
                )
            )

            jobs.add(
                raftCommandSubmitter.launchAppendEntriesSender(
                    coroutineContext = CoroutineName("${nodeIdentifier}|append-entries-sender"),
                    coroutineScope = coroutineScope,
                    period = 1000L..1000L
                )
            )

            return raftCommandSubmitter
        }
    }

    suspend fun buildNodes(
        coroutineScope: TestCoroutineScope,
        n: Int,
        action: suspend RaftClusterScope.(List<RaftCommandSubmitter>) -> Unit
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