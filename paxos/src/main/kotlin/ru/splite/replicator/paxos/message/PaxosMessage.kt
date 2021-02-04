package ru.splite.replicator.paxos.message

import kotlinx.serialization.Serializable
import ru.splite.replicator.log.LogEntry

@Serializable
sealed class PaxosMessage {

    @Serializable
    data class VoteRequest(
        /**
         * candidate’s term
         */
        val term: Long,
        /**
         * candidate’s commit index
         */
        val leaderCommit: Long
    ) : PaxosMessage()

    @Serializable
    data class VoteResponse(
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
        val entries: List<LogEntry>
    ) : PaxosMessage()
}