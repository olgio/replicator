package ru.splite.replicator.paxos

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.Command
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.bus.StubClusterTopology
import ru.splite.replicator.log.InMemoryReplicatedLogStore
import ru.splite.replicator.paxos.state.PaxosLocalNodeState
import ru.splite.replicator.raft.asMajority
import ru.splite.replicator.raft.hasOnlyCommands
import kotlin.test.Test

class PaxosControllerTests {

    @Test
    fun singleTermMultipleLogEntriesCommitTest(): Unit = runBlocking() {
        val clusterTopology = buildTopology()

        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        node2.apply {
            assertThat(sendVoteRequestsAsCandidate()).isTrue
            applyCommand(Command(1))
            applyCommand(Command(2))
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
    fun singleTermPartitionedLogEntriesCommitTest(): Unit = runBlocking() {
        val clusterTopology = buildTopology()

        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        node2.apply {
            assertThat(sendVoteRequestsAsCandidate()).isTrue
            applyCommand(Command(1))
            sendAppendEntriesIfLeader()
            commitLogEntriesIfLeader()
            assertThatLogs(node1).hasCommittedEntriesSize(0)

            applyCommand(Command(2))
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
            node1.applyCommand(Command(1))
            node1.sendAppendEntriesIfLeader()
            assertThatLogs(node2).hasOnlyCommands(1)
        }

        clusterTopology.isolateNodes(node2, node3) {
            assertThat(node2.sendVoteRequestsAsCandidate()).isTrue
            node2.sendAppendEntriesIfLeader()
            node2.sendAppendEntriesIfLeader()
            assertThatLogs(node3).hasOnlyCommands(1)

            node2.applyCommand(Command(2))
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
            applyCommand(Command(1))
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
            applyCommand(Command(1))
            sendAppendEntriesIfLeader()
        }

        node2.down()

        node3.apply {
            sendVoteRequestsAsCandidate()
            applyCommand(Command(2))
            applyCommand(Command(3))
            sendAppendEntriesIfLeader()
            commitLogEntriesIfLeader()
            sendAppendEntriesIfLeader()
        }

        assertThatLogs(node2).hasCommittedEntriesSize(0)
        assertThatLogs(node1, node3).hasCommittedEntriesSize(3)

        node2.up()

        node3.apply {
            sendAppendEntriesIfLeader()
            commitLogEntriesIfLeader()
            sendAppendEntriesIfLeader()
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
            node1.applyCommand(Command(1))
            node1.applyCommand(Command(2))
        }
        assertThatLogs(node1)
            .hasCommittedEntriesSize(0)
            .hasOnlyCommands(1, 2)
            .hasOnlyTerms(3L, 3L)

        clusterTopology.isolateNodes(node2, node3) {
            node3.apply {
                assertThat(sendVoteRequestsAsCandidate()).isTrue
                applyCommand(Command(3))
                applyCommand(Command(4))
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
            node1.applyCommand(Command(1))
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
            applyCommand(Command(1))
            sendAppendEntriesIfLeader()
        }

        clusterTopology.isolateNodes(node1, node2) {
            node1.applyCommand(Command(2))
            node1.sendAppendEntriesIfLeader()
        }
        assertThatLogs(node1, node2)
            .hasOnlyCommands(1, 2)
            .hasOnlyTerms(5L, 5L)

        clusterTopology.isolateNodes(node3, node4, node5) {
            assertThat(node3.sendVoteRequestsAsCandidate()).isTrue
            node3.sendAppendEntriesIfLeader()
            node3.applyCommand(Command(3))
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
            applyCommand(Command(1))
            sendAppendEntriesIfLeader()
        }

        clusterTopology.isolateNodes(node1, node2, node4) {
            node1.applyCommand(Command(2))
            node1.sendAppendEntriesIfLeader()
        }
        assertThatLogs(node1, node2, node4)
            .hasOnlyCommands(1, 2)
            .hasOnlyTerms(5L, 5L)

        clusterTopology.isolateNodes(node3, node4, node5) {
            assertThat(node3.sendVoteRequestsAsCandidate()).isTrue
            node3.sendAppendEntriesIfLeader()
            node3.applyCommand(Command(3))
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

    private fun buildTopology(): StubClusterTopology<ManagedPaxosProtocolNode<Command>> {
        return StubClusterTopology()
    }

    private fun StubClusterTopology<ManagedPaxosProtocolNode<Command>>.buildNode(
        name: String,
        n: Int,
        fullSize: Int
    ): ManagedPaxosProtocolNode<Command> {
        val nodeIdentifier = NodeIdentifier(name)
        val logStore = InMemoryReplicatedLogStore<Command>()
        val localNodeState = PaxosLocalNodeState<Command>(nodeIdentifier, n.toLong())
        val node = PaxosProtocolController(
            logStore,
            this,
            localNodeState,
            fullSize.asMajority(),
            fullSize.asMajority()
        ).asManaged()
        this[node.nodeIdentifier] = node
        return node
    }

    private fun StubClusterTopology<ManagedPaxosProtocolNode<Command>>.buildNodes(n: Int): List<ManagedPaxosProtocolNode<Command>> {
        return (0 until n).map {
            buildNode("node-$it", it, n)
        }
    }
}