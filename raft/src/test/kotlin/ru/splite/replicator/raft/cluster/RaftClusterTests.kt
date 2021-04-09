package ru.splite.replicator.raft.cluster

import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.demo.keyvalue.KeyValueCommand
import ru.splite.replicator.demo.keyvalue.KeyValueReply
import ru.splite.replicator.raft.assertThatLogs
import kotlin.test.Test

class RaftClusterTests {

    private val raftClusterBuilder = RaftClusterBuilder()

    @Test
    fun redirectToLeaderTest(): Unit = runBlockingTest {
        raftClusterBuilder.buildNodes(this, 3) { nodes ->

            advanceTimeBy(5000L)

            nodes.forEachIndexed { index, node ->
                val command = KeyValueCommand.newPutCommand("key", index.toString())
                val commandReply = KeyValueReply.deserializer(node.commandSubmitter.submit(command))
                assertThat(commandReply.value).isEqualTo(index.toString())
            }

            advanceTimeBy(5000L)

            assertThatLogs(*nodes.toTypedArray())
                .isCommittedEntriesInSync()
                .hasCommittedEntriesSize(3L)

            nodes.forEach {
                assertThat(it.stateMachine.currentState["key"]).isEqualTo("2")
            }
        }
    }

    @Test
    fun failedLeaderCommandReplicationTest(): Unit = runBlockingTest {
        raftClusterBuilder.buildNodes(this, 3) { nodes ->

            val command = KeyValueCommand.newPutCommand("1", "v")

            advanceTimeBy(5000L)

            val firstLeader =
                nodes.first { it.protocol.isLeader && !transport.isNodeIsolated(it.address) }

            val commandReply1 = KeyValueReply.deserializer(firstLeader.commandSubmitter.submit(command))
            assertThat(commandReply1.value).isEqualTo("v")

            advanceTimeBy(5000L)

            assertThatLogs(*nodes.map { it.protocol }.toTypedArray())
                .isCommittedEntriesInSync()
                .hasCommittedEntriesSize(1L)

            transport.setNodeIsolated(firstLeader.protocol.address, true)

            advanceTimeBy(5000L)

            val secondLeader =
                nodes.first { it.protocol.isLeader && !transport.isNodeIsolated(it.protocol.address) }

            val commandReply2 = KeyValueReply.deserializer(secondLeader.commandSubmitter.submit(command))
            assertThat(commandReply2.value).isEqualTo("v")

            advanceTimeBy(5000L)

            assertThatLogs(*nodes.filter { !transport.isNodeIsolated(it.protocol.address) }
                .map { it.protocol }.toTypedArray())
                .isCommittedEntriesInSync()
                .hasCommittedEntriesSize(2L)

            transport.setNodeIsolated(firstLeader.protocol.address, false)

            advanceTimeBy(5000L)

            assertThatLogs(*nodes.map { it.protocol }.toTypedArray())
                .isCommittedEntriesInSync()
                .hasCommittedEntriesSize(2L)
        }
    }
}