package ru.splite.replicator.raft

import kotlinx.coroutines.Job
import kotlinx.coroutines.test.TestCoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import kotlinx.serialization.Serializable
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.protobuf.ProtoBuf
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.log.InMemoryReplicatedLogStore
import ru.splite.replicator.raft.state.RaftLocalNodeState
import ru.splite.replicator.raft.state.asMajority
import ru.splite.replicator.statemachine.StateMachine
import ru.splite.replicator.timer.flow.DelayTimerFactory
import ru.splite.replicator.timer.flow.TimeTick
import ru.splite.replicator.transport.CoroutineChannelTransport
import kotlin.test.Test

class SubmitCommandTests {

    class TestTransportScope(private val coroutineScope: TestCoroutineScope) {

        val transport = CoroutineChannelTransport(coroutineScope)

        private val jobs = mutableListOf<Job>()

        private fun buildNode(name: String, fullSize: Int): RaftCommandSubmitter {
            val timerFactory = DelayTimerFactory(currentTimeTick = { TimeTick(coroutineScope.currentTime) })

            val nodeIdentifier = NodeIdentifier(name)
            val logStore = InMemoryReplicatedLogStore()
            val localNodeState = RaftLocalNodeState(nodeIdentifier)
            val stateMachine = KeyValueStateMachine()

            val raftProtocol = RaftProtocolController(
                logStore,
                transport,
                localNodeState,
                fullSize.asMajority(),
                fullSize.asMajority()
            )

            val termClockScheduler = TermClockScheduler(raftProtocol, timerFactory)
            jobs.add(termClockScheduler.launchTermClock(coroutineScope, 3000L..4000L))

            val raftCommandSubmitter = RaftCommandSubmitter(raftProtocol, stateMachine, timerFactory)
            jobs.add(raftCommandSubmitter.launchCommandApplier(coroutineScope))
            jobs.add(raftCommandSubmitter.launchAppendEntriesSender(coroutineScope, 1000L..1000L))

            return raftCommandSubmitter
        }

        suspend fun buildNodes(n: Int, action: suspend TestTransportScope.(List<RaftCommandSubmitter>) -> Unit) {
            val nodes = (1..n).map {
                buildNode("node-$it", n)
            }
            action.invoke(this, nodes)
            jobs.forEach { it.cancel() }
        }
    }

    @Serializable
    data class KeyValuePut(val key: String, val value: String)

    class KeyValueStateMachine : StateMachine<ByteArray, ByteArray> {

        private val store = mutableMapOf<String, String>()

        override fun commit(bytes: ByteArray): ByteArray {
            val command = ProtoBuf.decodeFromByteArray<KeyValuePut>(bytes)
            store[command.key] = command.value
            return bytes
        }

    }

    @Test
    fun consumerTest(): Unit = runBlockingTest {
        TestTransportScope(this).buildNodes(3) { nodes ->

            advanceTimeBy(5000L)

            val leader = nodes.first { it.raftProtocol.isLeader }

            val cmd = ProtoBuf.encodeToByteArray(KeyValuePut("1", "v"))
            val result = leader.submit(cmd)
            assertThat(cmd).isEqualTo(result)

            advanceTimeBy(5000L)

            assertThatLogs(*nodes.map { it.raftProtocol }.toTypedArray())
                .isCommittedEntriesInSync()
                .hasCommittedEntriesSize(1L)

            transport.isolate(leader.raftProtocol.nodeIdentifier)

            advanceTimeBy(5000L)

            val leader2 =
                nodes.first { it.raftProtocol.nodeIdentifier != leader.raftProtocol.nodeIdentifier && it.raftProtocol.isLeader }

            assertThat(cmd).isEqualTo(leader2.submit(cmd))

            advanceTimeBy(5000L)

            assertThatLogs(*nodes.filter { it.raftProtocol.nodeIdentifier != leader.raftProtocol.nodeIdentifier }
                .map { it.raftProtocol }.toTypedArray())
                .isCommittedEntriesInSync()
                .hasCommittedEntriesSize(2L)

            transport.open(leader.raftProtocol.nodeIdentifier)

            advanceTimeBy(5000L)

            assertThatLogs(*nodes.map { it.raftProtocol }.toTypedArray())
                .isCommittedEntriesInSync()
                .hasCommittedEntriesSize(2L)

        }
    }
}