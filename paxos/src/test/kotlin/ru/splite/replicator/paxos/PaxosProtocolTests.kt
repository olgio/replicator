package ru.splite.replicator.paxos

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.Command
import ru.splite.replicator.hasOnlyCommands
import ru.splite.replicator.log.InMemoryReplicatedLogStore
import ru.splite.replicator.paxos.state.PaxosLocalNodeState
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.transport.CoroutineChannelTransport
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.Transport
import ru.splite.replicator.transport.isolateNodes
import kotlin.test.Test

class PaxosProtocolTests {

    @Test
    fun singleTermMultipleLogEntriesCommitTest(): Unit = runBlocking {
        val clusterTopology = buildTopology()

        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        node2.apply {
            assertThat(sendVoteRequestsAsCandidate()).isTrue
            applyCommand(newCommand(1))
            applyCommand(newCommand(2))
            sendAppendEntriesIfLeader()
            commitLogEntriesIfLeader()
            sendAppendEntriesIfLeader()
        }

        assertThatLogs(node1, node2, node3)
            .hasOnlyCommands(1, 2)
            .hasCommittedEntriesSize(2)
            .isCommittedEntriesInSync()
    }

    @Test
    fun singleTermPartitionedLogEntriesCommitTest(): Unit = runBlocking {
        val clusterTopology = buildTopology()

        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        node2.apply {
            assertThat(sendVoteRequestsAsCandidate()).isTrue
            applyCommand(newCommand(1))
            sendAppendEntriesIfLeader()
            commitLogEntriesIfLeader()
            assertThatLogs(node1).hasCommittedEntriesSize(0)

            applyCommand(newCommand(2))
            sendAppendEntriesIfLeader()
            commitLogEntriesIfLeader()
            sendAppendEntriesIfLeader()
        }

        assertThatLogs(node1, node2, node3)
            .hasOnlyCommands(1, 2)
            .hasCommittedEntriesSize(2)
            .isCommittedEntriesInSync()
    }

    @Test
    fun twoTermPartitionedLogEntriesCommitTest(): Unit = runBlocking {
        val clusterTopology = buildTopology()

        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        clusterTopology.isolateNodes(node1, node2) {
            assertThat(node1.sendVoteRequestsAsCandidate()).isTrue
            node1.applyCommand(newCommand(1))
            node1.sendAppendEntriesIfLeader()
            assertThatLogs(node2).hasOnlyCommands(1)
        }

        clusterTopology.isolateNodes(node2, node3) {
            assertThat(node2.sendVoteRequestsAsCandidate()).isTrue
            node2.sendAppendEntriesIfLeader()
            node2.sendAppendEntriesIfLeader()
            assertThatLogs(node3).hasOnlyCommands(1)

            node2.applyCommand(newCommand(2))
            node2.sendAppendEntriesIfLeader()
            assertThatLogs(node3).hasOnlyCommands(1, 2)
        }
    }

    @Test
    fun commitFromPreviousTermTest(): Unit = runBlocking {
        val clusterTopology = buildTopology()

        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        node2.apply {
            assertThat(sendVoteRequestsAsCandidate()).isTrue
            applyCommand(newCommand(1))
            sendAppendEntriesIfLeader()

            assertThatLogs(node1, node2, node3)
                .hasCommittedEntriesSize(0)
                .hasOnlyCommands(1L)
                .hasOnlyTerms(1L)
        }

        node3.apply {
            assertThat(sendVoteRequestsAsCandidate()).isTrue
            commitLogEntriesIfLeader()

            assertThatLogs(node1, node2, node3)
                .hasCommittedEntriesSize(0)

            sendAppendEntriesIfLeader()
            commitLogEntriesIfLeader()
            sendAppendEntriesIfLeader()

            assertThatLogs(node1, node2, node3)
                .hasCommittedEntriesSize(1)
                .isCommittedEntriesInSync()
                .hasOnlyCommands(1L)
                .hasOnlyTerms(2L)
        }
    }

    @Test
    fun nodeDownUpTest(): Unit = runBlocking {
        val clusterTopology = buildTopology()

        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        node2.apply {
            sendVoteRequestsAsCandidate()
            applyCommand(newCommand(1))
            sendAppendEntriesIfLeader()
        }

        clusterTopology.isolateNodes(node1, node3) {
            node3.apply {
                sendVoteRequestsAsCandidate()
                applyCommand(newCommand(2))
                applyCommand(newCommand(3))
                sendAppendEntriesIfLeader()
                commitLogEntriesIfLeader()
                sendAppendEntriesIfLeader()
            }
        }

        assertThatLogs(node2).hasCommittedEntriesSize(0)
        assertThatLogs(node1, node3).hasCommittedEntriesSize(3)

        clusterTopology.isolateNodes(node1, node2, node3) {
            node3.apply {
                sendAppendEntriesIfLeader()
                commitLogEntriesIfLeader()
                sendAppendEntriesIfLeader()
            }
        }
        assertThatLogs(node2).hasCommittedEntriesSize(3)
    }

    @Test
    fun multiLeaderOnDownTest(): Unit = runBlocking {
        val clusterTopology = buildTopology()
        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        assertThat(node1.sendVoteRequestsAsCandidate()).isTrue
        node1.sendAppendEntriesIfLeader()
        clusterTopology.isolateNodes(node1) {
            node1.applyCommand(newCommand(1))
            node1.applyCommand(newCommand(2))
        }
        assertThatLogs(node1)
            .hasCommittedEntriesSize(0)
            .hasOnlyCommands(1, 2)
            .hasOnlyTerms(3L, 3L)

        clusterTopology.isolateNodes(node2, node3) {
            node3.apply {
                assertThat(sendVoteRequestsAsCandidate()).isTrue
                applyCommand(newCommand(3))
                applyCommand(newCommand(4))
                sendAppendEntriesIfLeader()
                commitLogEntriesIfLeader()
                sendAppendEntriesIfLeader()
            }
            assertThatLogs(node2, node3)
                .hasCommittedEntriesSize(2)
                .hasOnlyCommands(3, 4)
                .hasOnlyTerms(5L, 5L)
            assertThatLogs(node1)
                .hasCommittedEntriesSize(0)
        }

        clusterTopology.isolateNodes(node1, node2, node3) {
            node1.sendAppendEntriesIfLeader()
            node3.sendAppendEntriesIfLeader()
            assertThatLogs(node1, node2, node3)
                .hasCommittedEntriesSize(2)
                .hasOnlyCommands(3, 4)
                .isCommittedEntriesInSync()
        }
    }

    @Test
    fun cannotElectLeaderIfNoMajority(): Unit = runBlocking {
        val clusterTopology = buildTopology()
        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        clusterTopology.isolateNodes(node1) {
            assertThat(node1.sendVoteRequestsAsCandidate()).isFalse
        }
    }

    @Test
    fun shouldElectLeaderIfMajority(): Unit = runBlocking {
        val clusterTopology = buildTopology()
        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        clusterTopology.isolateNodes(node1, node2) {
            assertThat(node1.sendVoteRequestsAsCandidate()).isTrue
        }
    }

    @Test
    fun cannotCommitLogEntryIfNoMajority(): Unit = runBlocking {
        val clusterTopology = buildTopology()
        val (node1, node2, node3) = clusterTopology.buildNodes(3)
        assertThat(node1.sendVoteRequestsAsCandidate()).isTrue

        clusterTopology.isolateNodes(node1) {
            node1.applyCommand(newCommand(1))
            repeat(2) {
                node1.sendAppendEntriesIfLeader()
                node1.commitLogEntriesIfLeader()
            }
            assertThatLogs(node1, node2, node3).hasCommittedEntriesSize(0)
        }

        clusterTopology.isolateNodes(node1, node2, node3) {
            repeat(2) {
                node1.sendAppendEntriesIfLeader()
                node1.commitLogEntriesIfLeader()
            }
            assertThatLogs(node1, node2, node3).hasCommittedEntriesSize(1)
        }
    }

    @Test
    fun overrideLogEntryIfNotAppendedOnMajority(): Unit = runBlocking {
        val clusterTopology = buildTopology()

        val (node1, node2, node3, node4, node5) = clusterTopology.buildNodes(5)

        node1.apply {
            assertThat(sendVoteRequestsAsCandidate()).isTrue
            applyCommand(newCommand(1))
            sendAppendEntriesIfLeader()
        }

        clusterTopology.isolateNodes(node1, node2) {
            node1.applyCommand(newCommand(2))
            node1.sendAppendEntriesIfLeader()
        }
        assertThatLogs(node1, node2)
            .hasOnlyCommands(1, 2)
            .hasOnlyTerms(5L, 5L)

        clusterTopology.isolateNodes(node3, node4, node5) {
            assertThat(node3.sendVoteRequestsAsCandidate()).isTrue
            node3.sendAppendEntriesIfLeader()
            node3.applyCommand(newCommand(3))
            node3.sendAppendEntriesIfLeader()
            node3.commitLogEntriesIfLeader()
            node3.sendAppendEntriesIfLeader()
        }
        assertThatLogs(node3, node4, node5)
            .hasOnlyCommands(1, 3)
            .hasOnlyTerms(7L, 7L)

        clusterTopology.isolateNodes(node1, node2, node3, node4, node5) {
            node3.sendAppendEntriesIfLeader()
        }

        assertThatLogs(node1, node2, node3, node4, node5)
            .hasCommittedEntriesSize(2L)
            .isCommittedEntriesInSync()
            .hasOnlyCommands(1, 3)
            .hasOnlyTerms(7L, 7L)
    }

    @Test
    fun cannotOverrideLogEntryIfAppendedOnMajority(): Unit = runBlocking {
        val clusterTopology = buildTopology()

        val (node1, node2, node3, node4, node5) = clusterTopology.buildNodes(5)

        node1.apply {
            assertThat(sendVoteRequestsAsCandidate()).isTrue
            applyCommand(newCommand(1))
            sendAppendEntriesIfLeader()
        }

        clusterTopology.isolateNodes(node1, node2, node4) {
            node1.applyCommand(newCommand(2))
            node1.sendAppendEntriesIfLeader()
        }
        assertThatLogs(node1, node2, node4)
            .hasOnlyCommands(1, 2)
            .hasOnlyTerms(5L, 5L)

        clusterTopology.isolateNodes(node3, node4, node5) {
            assertThat(node3.sendVoteRequestsAsCandidate()).isTrue
            node3.sendAppendEntriesIfLeader()
            node3.applyCommand(newCommand(3))
            node3.sendAppendEntriesIfLeader()
            node3.commitLogEntriesIfLeader()
            node3.sendAppendEntriesIfLeader()
        }
        assertThatLogs(node3, node4, node5)
            .hasOnlyCommands(1, 2, 3)
            .hasOnlyTerms(7L, 7L, 7L)

        clusterTopology.isolateNodes(node1, node2, node3, node4, node5) {
            node3.sendAppendEntriesIfLeader()
        }

        assertThatLogs(node1, node2, node3, node4, node5)
            .hasCommittedEntriesSize(3L)
            .isCommittedEntriesInSync()
            .hasOnlyCommands(1, 2, 3)
            .hasOnlyTerms(7L, 7L, 7L)
    }

    private fun newCommand(value: Long): ByteArray {
        return Command.Serializer.serialize(Command(value))
    }

    private fun CoroutineScope.buildTopology(): CoroutineChannelTransport {
        return CoroutineChannelTransport(this)
    }

    private fun Transport.buildNode(
        name: String,
        n: Int,
        fullSize: Int
    ): PaxosProtocolController {
        val nodeIdentifier = NodeIdentifier(name)
        val logStore = InMemoryReplicatedLogStore()
        val localNodeState = PaxosLocalNodeState(nodeIdentifier, n.toLong())
        val config = RaftProtocolConfig(n = fullSize)
        return PaxosProtocolController(
            logStore,
            this,
            config,
            localNodeState
        )
    }

    private fun Transport.buildNodes(n: Int): List<PaxosProtocolController> {
        return (0 until n).map {
            buildNode("node-$it", it, n)
        }
    }
}