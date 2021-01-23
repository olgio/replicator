package ru.splite.replicator.raft

import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.raft.log.InMemoryReplicatedLogStore
import ru.splite.replicator.raft.log.ReplicatedLogStore
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.RaftStateMutationManager
import kotlin.test.Test

class RaftStateTransitionsTests {

    private data class Command(val value: Long = 0)

    @Test
    fun test() {
        val replicatedLogStore1: ReplicatedLogStore<Command> = InMemoryReplicatedLogStore()
        val node1 = RaftStateMutationManager(NodeIdentifier("node-1"), replicatedLogStore1)

        val replicatedLogStore2: ReplicatedLogStore<Command> = InMemoryReplicatedLogStore()
        val node2 = RaftStateMutationManager(NodeIdentifier("node-2"), replicatedLogStore2)

        val replicatedLogStore3: ReplicatedLogStore<Command> = InMemoryReplicatedLogStore()
        val node3 = RaftStateMutationManager(NodeIdentifier("node-3"), replicatedLogStore3)

        /*
        Leader election
         */
        run {
            val voteRequest = node1.becomeCandidate(1L)

            val voteResponse2 = node2.handleVoteRequest(voteRequest)
            val voteResponse3 = node3.handleVoteRequest(voteRequest)

            assertThat(voteResponse2.voteGranted).isTrue
            assertThat(voteResponse3.voteGranted).isTrue

            node1.becomeLeader()

            assertThat(node1.currentNodeType).isEqualTo(NodeType.LEADER)
            assertThat(node2.currentNodeType).isEqualTo(NodeType.FOLLOWER)
            assertThat(node3.currentNodeType).isEqualTo(NodeType.FOLLOWER)

            assertThat(node1.currentTerm).isEqualTo(1)
            assertThat(node2.currentTerm).isEqualTo(1)
            assertThat(node3.currentTerm).isEqualTo(1)
        }

        /*
        Command 1 replication
         */

        run {
            node1.addCommand(Command(1))

            run {
                val appendEntriesRequest = node1.buildAppendEntries(0)

                assertThat(appendEntriesRequest.entries).hasSize(1)
                assertThat(appendEntriesRequest.lastCommitIndex).isNull()

                val appendEntriesResponse2 = node2.handleAppendEntries(appendEntriesRequest)
                val appendEntriesResponse3 = node3.handleAppendEntries(appendEntriesRequest)

                assertThat(appendEntriesResponse2.entriesAppended).isTrue
                assertThat(appendEntriesResponse3.entriesAppended).isTrue
            }

            assertThat(node1.leaderIdentifier).isEqualTo(node1.nodeIdentifier)
            assertThat(node2.leaderIdentifier).isEqualTo(node1.nodeIdentifier)
            assertThat(node3.leaderIdentifier).isEqualTo(node1.nodeIdentifier)


            replicatedLogStore1.commit(0L)

            run {
                val appendEntriesRequest = node1.buildAppendEntries(1)

                assertThat(appendEntriesRequest.lastCommitIndex).isEqualTo(0)

                val appendEntriesResponse2 = node2.handleAppendEntries(appendEntriesRequest)
                val appendEntriesResponse3 = node3.handleAppendEntries(appendEntriesRequest)

                assertThat(appendEntriesResponse2.entriesAppended).isTrue
                assertThat(appendEntriesResponse3.entriesAppended).isTrue
            }
        }

        // Command 2 replication

        run {

            node1.addCommand(Command(2))

            run {
                val appendEntriesRequest = node1.buildAppendEntries(1)

                assertThat(appendEntriesRequest.entries).hasSize(1)
                assertThat(appendEntriesRequest.lastCommitIndex).isEqualTo(0)

                val appendEntriesResponse2 = node2.handleAppendEntries(appendEntriesRequest)
                val appendEntriesResponse3 = node3.handleAppendEntries(appendEntriesRequest)

                assertThat(appendEntriesResponse2.entriesAppended).isTrue
                assertThat(appendEntriesResponse3.entriesAppended).isTrue
            }


            replicatedLogStore1.commit(1L)

            run {
                val appendEntriesRequest = node1.buildAppendEntries(2)

                assertThat(appendEntriesRequest.lastCommitIndex).isEqualTo(1)

                val appendEntriesResponse2 = node2.handleAppendEntries(appendEntriesRequest)
                val appendEntriesResponse3 = node3.handleAppendEntries(appendEntriesRequest)

                assertThat(appendEntriesResponse2.entriesAppended).isTrue
                assertThat(appendEntriesResponse3.entriesAppended).isTrue
            }
        }

        assertThat(replicatedLogStore1.lastCommitIndex()).isEqualTo(1)
        assertThat(replicatedLogStore2.lastCommitIndex()).isEqualTo(1)
        assertThat(replicatedLogStore3.lastCommitIndex()).isEqualTo(1)

        run {
            val voteRequest = node2.becomeCandidate(node2.currentTerm + 1)

            val voteResponse3 = node3.handleVoteRequest(voteRequest)

            assertThat(voteResponse3.voteGranted).isTrue

            node2.becomeLeader()

            assertThat(node2.currentNodeType).isEqualTo(NodeType.LEADER)
            assertThat(node3.currentNodeType).isEqualTo(NodeType.FOLLOWER)

            assertThat(node2.currentTerm).isEqualTo(2)
            assertThat(node3.currentTerm).isEqualTo(2)
        }

    }

}