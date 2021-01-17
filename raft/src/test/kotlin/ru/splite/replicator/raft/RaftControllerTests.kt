package ru.splite.replicator.raft

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.raft.message.StubClusterTopology
import kotlin.test.Test

class RaftControllerTests {

    private data class Command(val value: Long = 0)

    @Test
    fun singleTermMultipleLogEntriesCommitTest() = runBlocking() {
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
        assertLogEntriesOnNode(0, node1, 1, 2)
        assertCommittedInSyncOnNodes(1, node1, node2, node3)
    }

    @Test
    fun singleTermPartitionedLogEntriesCommitTest() = runBlocking() {
        val clusterTopology = buildTopology()

        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        node2.apply {
            assertThat(sendVoteRequestsAsCandidate()).isTrue
            applyCommand(Command(1))
            sendAppendEntriesIfLeader()
            commitLogEntriesIfLeader()
            assertLogEntriesOnNode(0, node1, 1)

            applyCommand(Command(2))
            sendAppendEntriesIfLeader()
            commitLogEntriesIfLeader()
            sendAppendEntriesIfLeader()
        }
        assertLogEntriesOnNode(0, node1, 1, 2)
        assertCommittedInSyncOnNodes(1, node1, node2, node3)
    }

    @Test
    fun twoTermPartitionedLogEntriesCommitTest() = runBlocking {
        val clusterTopology = buildTopology()

        val (node1, node2, node3) = clusterTopology.buildNodes(3)

        clusterTopology.isolateNodes(node1, node2) {
            assertThat(node1.sendVoteRequestsAsCandidate()).isTrue
            node1.applyCommand(Command(1))
            node1.sendAppendEntriesIfLeader()
            assertLogEntriesOnNode(0, node2, 1)
        }

        clusterTopology.isolateNodes(node2, node3) {
            assertThat(node2.sendVoteRequestsAsCandidate()).isTrue
            node2.sendAppendEntriesIfLeader()
            node2.sendAppendEntriesIfLeader()
            assertLogEntriesOnNode(0, node3, 1)
            node2.applyCommand(Command(2))
            node2.sendAppendEntriesIfLeader()
            assertLogEntriesOnNode(0, node3, 1, 2)
        }
    }

    @Test
    fun commitFromPreviousTermTest() {
        runBlocking {
            val clusterTopology = buildTopology()

            val (node1, node2, node3) = clusterTopology.buildNodes(3)

            node2.apply {
                sendVoteRequestsAsCandidate()
                applyCommand(Command(1))
                sendAppendEntriesIfLeader()
            }
            assertCommittedInSyncOnNodes(null, node1, node2, node3)

            node3.apply {
                sendVoteRequestsAsCandidate()
                applyCommand(Command(2))
                applyCommand(Command(3))
                sendAppendEntriesIfLeader()
                commitLogEntriesIfLeader()
                sendAppendEntriesIfLeader()
            }
            assertCommittedInSyncOnNodes(2, node1, node2, node3)
        }
    }

    @Test
    fun nodeDownUpTest() {
        runBlocking {
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

            assertCommittedInSyncOnNodes(null, node2)
            assertCommittedInSyncOnNodes(2, node1, node3)

            node2.up()

            node3.apply {
                sendAppendEntriesIfLeader()
                commitLogEntriesIfLeader()
                sendAppendEntriesIfLeader()
            }
            assertCommittedInSyncOnNodes(2, node2)
        }
    }

    @Test
    fun multiLeaderOnDownTest() {
        runBlocking {
            val clusterTopology = buildTopology()
            val (node1, node2, node3) = clusterTopology.buildNodes(3)

            assertThat(node1.sendVoteRequestsAsCandidate()).isTrue

            clusterTopology.isolateNodes(node1) {
                node1.apply {
                    applyCommand(Command(1))
                    applyCommand(Command(2))
                }
            }

            clusterTopology.isolateNodes(node2, node3) {
                node3.apply {
                    sendVoteRequestsAsCandidate()
                    applyCommand(Command(3))
                    applyCommand(Command(4))
                    sendAppendEntriesIfLeader()
                    commitLogEntriesIfLeader()
                    sendAppendEntriesIfLeader()
                }
                assertCommittedInSyncOnNodes(1, node2, node3)
                assertCommittedInSyncOnNodes(null, node1)
            }

            clusterTopology.isolateNodes(node1, node2, node3) {
                node1.sendAppendEntriesIfLeader()
                node3.sendAppendEntriesIfLeader()
                assertCommittedInSyncOnNodes(1, node1, node2, node3)
            }
        }
    }

    @Test
    fun cannotElectLeaderIfNoMajority() {
        runBlocking {
            val clusterTopology = buildTopology()
            val (node1, node2, node3) = clusterTopology.buildNodes(3)

            clusterTopology.isolateNodes(node1) {
                assertThat(node1.sendVoteRequestsAsCandidate()).isFalse
            }
        }
    }

    @Test
    fun shouldElectLeaderIfMajority() {
        runBlocking {
            val clusterTopology = buildTopology()
            val (node1, node2, node3) = clusterTopology.buildNodes(3)

            clusterTopology.isolateNodes(node1, node2) {
                assertThat(node1.sendVoteRequestsAsCandidate()).isTrue
            }
        }
    }

    @Test
    fun cannotCommitLogEntryIfNoMajority() {
        runBlocking {
            val clusterTopology = buildTopology()
            val (node1, node2, node3) = clusterTopology.buildNodes(3)
            assertThat(node1.sendVoteRequestsAsCandidate()).isTrue

            clusterTopology.isolateNodes(node1) {
                node1.applyCommand(Command(1))
                repeat(2) {
                    node1.sendAppendEntriesIfLeader()
                    node1.commitLogEntriesIfLeader()
                }
                assertCommittedInSyncOnNodes(null, node1, node2, node3)
            }

            clusterTopology.isolateNodes(node1, node2, node3) {
                repeat(2) {
                    node1.sendAppendEntriesIfLeader()
                    node1.commitLogEntriesIfLeader()
                }
                assertCommittedInSyncOnNodes(0, node1, node2, node3)
            }
        }
    }

    @Test
    fun overrideLogEntryAppendedOnMajorityIfFailedBeforeCommit() {
        runBlocking {
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
            assertLogEntriesOnNode(0, node2, 1, 2)

            clusterTopology.isolateNodes(node2, node3, node4, node5) {
                assertThat(node5.sendVoteRequestsAsCandidate()).isTrue
                node5.sendAppendEntriesIfLeader()
                node5.applyCommand(Command(3))
            }
            assertLogEntriesOnNode(0, node5, 1, 3)

            clusterTopology.isolateNodes(node1, node2, node3) {
                assertThat(node1.sendVoteRequestsAsCandidate()).isTrue
                repeat(2) {
                    node1.sendAppendEntriesIfLeader()
                }
            }
            assertLogEntriesOnNode(0, node2, 1, 2)
            assertLogEntriesOnNode(0, node3, 1, 2)

            clusterTopology.isolateNodes(node1, node2, node3, node4, node5) {
                assertThat(node5.sendVoteRequestsAsCandidate()).isTrue
                node5.applyCommand(Command(5))

                repeat(3) {
                    node5.sendAppendEntriesIfLeader()
                    node5.commitLogEntriesIfLeader()
                }
            }
            assertLogEntriesOnNode(0, node3, 1, 3, 5)
            assertCommittedInSyncOnNodes(2, node1, node2, node3, node4, node5)
        }
    }

    @Test
    fun cannotOverrideLogEntryAppendedOnMajorityIfNotFailedBeforeCommit() {
        runBlocking {
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
            assertLogEntriesOnNode(0, node2, 1, 2)

            clusterTopology.isolateNodes(node2, node3, node4, node5) {
                assertThat(node5.sendVoteRequestsAsCandidate()).isTrue
                node5.sendAppendEntriesIfLeader()
                node5.applyCommand(Command(3))
            }
            assertLogEntriesOnNode(0, node5, 1, 3)

            clusterTopology.isolateNodes(node1, node2, node3) {
                assertThat(node1.sendVoteRequestsAsCandidate()).isTrue
                node1.applyCommand(Command(4))
                repeat(2) {
                    node1.sendAppendEntriesIfLeader()
                }
            }
            clusterTopology.isolateNodes(node1, node2, node3, node4, node5) {
                assertThat(node5.sendVoteRequestsAsCandidate()).isFalse
            }

            assertCommittedInSyncOnNodes(null, node1, node2, node3, node4, node5)
        }
    }

    private fun assertLogEntriesOnNode(fromIndex: Long, node: RaftProtocol<Command>, vararg values: Long) {
        values.forEachIndexed { index, value ->
            assertThat(node.replicatedLogStore.getLogEntryByIndex(fromIndex + index)?.command?.value).isEqualTo(value)
        }
    }

    private fun assertCommittedInSyncOnNodes(expectedLastCommitIndex: Long?, vararg nodes: RaftProtocol<Command>) {
        nodes.forEach {
            val lastCommitIndex = it.replicatedLogStore.lastCommitIndex()
            assertThat(lastCommitIndex).withFailMessage("${it.nodeIdentifier} has unexpected lastCommitIndex = ${lastCommitIndex}")
                .isEqualTo(
                    expectedLastCommitIndex
                )
        }
        if (expectedLastCommitIndex != null) {
            (0..expectedLastCommitIndex).forEach { index ->
                assertThat(nodes.map { it.replicatedLogStore.getLogEntryByIndex(index) }.toSet()).hasSize(1)
            }
        }
    }

    private fun buildTopology(): StubClusterTopology<ManagedRaftProtocolNode<Command>> {
        return StubClusterTopology()
    }

    private fun StubClusterTopology<ManagedRaftProtocolNode<Command>>.buildNode(name: String): ManagedRaftProtocolNode<Command> {
        val node = RaftProtocolController(this, NodeIdentifier(name)).asManaged()
        this[node.nodeIdentifier] = node
        return node
    }

    private fun StubClusterTopology<ManagedRaftProtocolNode<Command>>.buildNodes(n: Int): List<ManagedRaftProtocolNode<Command>> {
        return (1..n).map {
            buildNode("node-$it")
        }
    }
}