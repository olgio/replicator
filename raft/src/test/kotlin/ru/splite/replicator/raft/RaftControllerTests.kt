package ru.splite.replicator.raft

import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.raft.message.StubClusterTopology
import kotlin.test.Test

class RaftControllerTests {

    private data class Command(val value: Long = 0)

    @Test
    fun singleTermLogEntriesCommitTest() = runBlocking() {
        val clusterTopology = StubClusterTopology<Command>()

        val node1 = clusterTopology.buildNode("node-1")
        val node2 = clusterTopology.buildNode("node-2")
        val node3 = clusterTopology.buildNode("node-3")

        node2.apply {
            sendVoteRequestsAsCandidate()
            applyCommand(Command(1))
            applyCommand(Command(2))
        }

        repeat(2) {
            node2.sendAppendEntriesIfLeader()
            node2.commitLogEntriesIfLeader()
        }

        assertThatNodesInSync(1, node1, node2, node3)
    }

    @Test
    fun commitFromPreviousTermTest() {
        runBlocking {
            val clusterTopology = StubClusterTopology<Command>()

            val node1 = clusterTopology.buildNode("node-1")
            val node2 = clusterTopology.buildNode("node-2")
            val node3 = clusterTopology.buildNode("node-3")

            node2.apply {
                sendVoteRequestsAsCandidate()
                applyCommand(Command(1))
                sendAppendEntriesIfLeader()
            }

            node3.apply {
                sendVoteRequestsAsCandidate()
                applyCommand(Command(2))
                applyCommand(Command(3))
                commitLogEntriesIfLeader()
                sendAppendEntriesIfLeader()
            }

            assertThatNodesInSync(2, node1, node2, node3)
        }
    }

    private fun <C> assertThatNodesInSync(expectedLastCommitIndex: Long, vararg nodes: RaftProtocolController<C>) {
        nodes.forEach {
            val lastCommitIndex = it.replicatedLogStore.lastCommitIndex()
            assertThat(lastCommitIndex).withFailMessage("${it.nodeIdentifier} has unexpected lastCommitIndex = ${lastCommitIndex}").isNotNull.isEqualTo(
                expectedLastCommitIndex
            )
            assertThat(it.replicatedLogStore.getLogEntryByIndex(it.replicatedLogStore.lastCommitIndex()!!)).isNotNull
        }
    }

    private fun StubClusterTopology<Command>.buildNode(name: String): RaftProtocolController<Command> {
        val node = RaftProtocolController(this, NodeIdentifier(name))
        this[node.nodeIdentifier] = node
        return node
    }
}