package ru.splite.replicator.raft

import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import kotlin.test.Test

class RaftClusterTests {

    private val raftClusterBuilder = RaftClusterBuilder()

    @Test
    fun failedLeaderCommandReplicationTest(): Unit = runBlockingTest {
        raftClusterBuilder.buildNodes(this, 3) { nodes ->

            val cmd = KeyValueStateMachine.serialize(KeyValueStateMachine.PutCommand("1", "v"))

            advanceTimeBy(5000L)

            val firstLeader =
                nodes.first { it.raftProtocol.isLeader && !transport.isNodeIsolated(it.raftProtocol.nodeIdentifier) }

            val result = firstLeader.submit(cmd)
            assertThat(cmd).isEqualTo(result)

            advanceTimeBy(5000L)

            assertThatLogs(*nodes.map { it.raftProtocol }.toTypedArray())
                .isCommittedEntriesInSync()
                .hasCommittedEntriesSize(1L)

            transport.setNodeIsolated(firstLeader.raftProtocol.nodeIdentifier, true)

            advanceTimeBy(5000L)

            val secondLeader =
                nodes.first { it.raftProtocol.isLeader && !transport.isNodeIsolated(it.raftProtocol.nodeIdentifier) }

            assertThat(cmd).isEqualTo(secondLeader.submit(cmd))

            advanceTimeBy(5000L)

            assertThatLogs(*nodes.filter { !transport.isNodeIsolated(it.raftProtocol.nodeIdentifier) }
                .map { it.raftProtocol }.toTypedArray())
                .isCommittedEntriesInSync()
                .hasCommittedEntriesSize(2L)

            transport.setNodeIsolated(firstLeader.raftProtocol.nodeIdentifier, false)

            advanceTimeBy(5000L)

            assertThatLogs(*nodes.map { it.raftProtocol }.toTypedArray())
                .isCommittedEntriesInSync()
                .hasCommittedEntriesSize(2L)
        }
    }
}