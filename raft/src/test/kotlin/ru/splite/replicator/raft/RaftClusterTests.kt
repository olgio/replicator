package ru.splite.replicator.raft

import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.keyvalue.KeyValueCommand
import ru.splite.replicator.keyvalue.KeyValueReply
import kotlin.test.Test

class RaftClusterTests {

    private val raftClusterBuilder = RaftClusterBuilder()

    @Test
    fun failedLeaderCommandReplicationTest(): Unit = runBlockingTest {
        raftClusterBuilder.buildNodes(this, 3) { nodes ->

            val command = KeyValueCommand.newPutCommand("1", "v")

            advanceTimeBy(5000L)

            val firstLeader =
                nodes.first { it.protocol.isLeader && !transport.isNodeIsolated(it.protocol.address) }

            val commandReply1 = KeyValueReply.deserializer(firstLeader.submit(command))
            assertThat(commandReply1.value).isEqualTo("v")

            advanceTimeBy(5000L)

            assertThatLogs(*nodes.map { it.protocol }.toTypedArray())
                .isCommittedEntriesInSync()
                .hasCommittedEntriesSize(1L)

            transport.setNodeIsolated(firstLeader.protocol.address, true)

            advanceTimeBy(5000L)

            val secondLeader =
                nodes.first { it.protocol.isLeader && !transport.isNodeIsolated(it.protocol.address) }

            val commandReply2 = KeyValueReply.deserializer(secondLeader.submit(command))
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