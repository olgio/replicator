package ru.splite.replicator.raft.message

import kotlinx.serialization.Serializable
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.transport.NodeIdentifier

@Serializable
sealed class RaftMessage {

    @Serializable
    data class VoteRequest(
        val term: Long,
        val candidateIdentifier: NodeIdentifier,
        val lastLogIndex: Long,
        val lastLogTerm: Long
    ) : RaftMessage()

    @Serializable
    data class VoteResponse(
        val term: Long,
        val voteGranted: Boolean
    ) : RaftMessage()

    @Serializable
    data class AppendEntries(
        val term: Long,
        val leaderIdentifier: NodeIdentifier,
        val prevLogIndex: Long,
        val prevLogTerm: Long,
        val lastCommitIndex: Long,
        val entries: List<LogEntry> = emptyList()
    ) : RaftMessage()

    @Serializable
    data class AppendEntriesResponse(
        val term: Long,
        val entriesAppended: Boolean
    ) : RaftMessage()

    @Serializable
    data class PaxosVoteRequest(
        /**
         * candidate’s term
         */
        val term: Long,
        /**
         * candidate’s commit index
         */
        val leaderCommit: Long
    ) : RaftMessage()

    @Serializable
    data class PaxosVoteResponse(
        /**
         * currentTerm, for candidate to update itself
         */
        val term: Long,
        /**
         * true indicates candidate received vote
         */
        val voteGranted: Boolean,
        /**
         * follower’s log entries after leaderCommit
         */
        val entries: List<LogEntry> = emptyList()
    ) : RaftMessage()

    @Serializable
    data class RedirectRequest(
        val command: ByteArray
    ) : RaftMessage() {

        override fun toString(): String {
            return "RedirectRequest(commandSize=${command.size})"
        }
    }

    @Serializable
    data class RedirectResponse(
        val index: Long,
        val term: Long
    ) : RaftMessage()
}
