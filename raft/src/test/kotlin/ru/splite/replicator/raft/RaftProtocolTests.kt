package ru.splite.replicator.raft

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.demo.Command
import ru.splite.replicator.demo.hasOnlyCommands
import ru.splite.replicator.log.InMemoryReplicatedLogStore
import ru.splite.replicator.raft.state.RaftLocalNodeState
import ru.splite.replicator.transport.CoroutineChannelTransport
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.Transport
import ru.splite.replicator.transport.isolateNodes
import kotlin.test.Test

class RaftProtocolTests {

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
            sendVoteRequestsAsCandidate()
            applyCommand(newCommand(1))
            sendAppendEntriesIfLeader()
        }
        assertThatLogs(node1, node2, node3).hasCommittedEntriesSize(0)

        node3.apply {
            sendVoteRequestsAsCandidate()
            applyCommand(newCommand(2))
            applyCommand(newCommand(3))
            sendAppendEntriesIfLeader()
            commitLogEntriesIfLeader()
            sendAppendEntriesIfLeader()
        }
        assertThatLogs(node1, node2, node3).hasCommittedEntriesSize(3)
            .isCommittedEntriesInSync()

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

            assertThatLogs(node2).hasCommittedEntriesSize(0)
            assertThatLogs(node1, node3).hasCommittedEntriesSize(3)
        }

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

        clusterTopology.isolateNodes(node1) {
            node1.apply {
                applyCommand(newCommand(1))
                applyCommand(newCommand(2))
            }
        }

        clusterTopology.isolateNodes(node2, node3) {
            node3.apply {
                sendVoteRequestsAsCandidate()
                applyCommand(newCommand(3))
                applyCommand(newCommand(4))
                sendAppendEntriesIfLeader()
                commitLogEntriesIfLeader()
                sendAppendEntriesIfLeader()
            }
            assertThatLogs(node2, node3).hasCommittedEntriesSize(2)
            assertThatLogs(node1).hasCommittedEntriesSize(0)

        }

        clusterTopology.isolateNodes(node1, node2, node3) {
            node1.sendAppendEntriesIfLeader()
            node3.sendAppendEntriesIfLeader()
            assertThatLogs(node1, node2, node3)
                .hasCommittedEntriesSize(2)
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
    fun overrideLogEntryAppendedOnMajorityIfFailedBeforeCommit(): Unit = runBlocking {
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
        assertThatLogs(node2).hasOnlyCommands(1, 2)

        clusterTopology.isolateNodes(node2, node3, node4, node5) {
            assertThat(node5.sendVoteRequestsAsCandidate()).isTrue
            node5.sendAppendEntriesIfLeader()
            node5.applyCommand(newCommand(3))
        }
        assertThatLogs(node5).hasOnlyCommands(1, 3)

        clusterTopology.isolateNodes(node1, node2, node3) {
            assertThat(node1.sendVoteRequestsAsCandidate()).isTrue
            repeat(2) {
                node1.sendAppendEntriesIfLeader()
            }
        }
        assertThatLogs(node2, node3).hasOnlyCommands(1, 2)

        clusterTopology.isolateNodes(node1, node2, node3, node4, node5) {
            assertThat(node5.sendVoteRequestsAsCandidate()).isTrue
            node5.applyCommand(newCommand(5))

            repeat(3) {
                node5.sendAppendEntriesIfLeader()
                node5.commitLogEntriesIfLeader()
            }
        }
        assertThatLogs(node3).hasOnlyCommands(1, 3, 5)

        assertThatLogs(node1, node2, node3, node4, node5)
            .hasCommittedEntriesSize(3)
            .isCommittedEntriesInSync()
    }

    @Test
    fun cannotOverrideLogEntryAppendedOnMajorityIfNotFailedBeforeCommit(): Unit = runBlocking {
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
        assertThatLogs(node2).hasOnlyCommands(1, 2)

        clusterTopology.isolateNodes(node2, node3, node4, node5) {
            assertThat(node5.sendVoteRequestsAsCandidate()).isTrue
            node5.sendAppendEntriesIfLeader()
            node5.applyCommand(newCommand(3))
        }
        assertThatLogs(node5).hasOnlyCommands(1, 3)

        clusterTopology.isolateNodes(node1, node2, node3) {
            assertThat(node1.sendVoteRequestsAsCandidate()).isTrue
            node1.applyCommand(newCommand(4))
            repeat(2) {
                node1.sendAppendEntriesIfLeader()
            }
        }
        clusterTopology.isolateNodes(node1, node2, node3, node4, node5) {
            assertThat(node5.sendVoteRequestsAsCandidate()).isFalse
        }

        assertThatLogs(node1, node2, node3, node4, node5)
            .hasCommittedEntriesSize(0)
            .isCommittedEntriesInSync()
    }

    private fun newCommand(value: Long): ByteArray {
        return Command.Serializer.serialize(Command(value))
    }

    private fun CoroutineScope.buildTopology(): CoroutineChannelTransport {
        return CoroutineChannelTransport(this)
    }

    private fun Transport.buildNode(name: String, fullSize: Int): RaftProtocolController {
        val nodeIdentifier = NodeIdentifier(name)
        val logStore = InMemoryReplicatedLogStore()
        val localNodeState = RaftLocalNodeState()
        val config = RaftProtocolConfig(address = nodeIdentifier, n = fullSize)
        val protocol = BaseRaftProtocol(logStore, config, localNodeState)
        return RaftProtocolController(this, config, protocol)
    }

    private fun Transport.buildNodes(n: Int): List<RaftProtocolController> {
        return (1..n).map {
            buildNode("node-$it", n)
        }
    }
}