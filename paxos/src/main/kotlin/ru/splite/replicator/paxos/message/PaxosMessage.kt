package ru.splite.replicator.paxos.message

import ru.splite.replicator.log.LogEntry

sealed class PaxosMessage(open val term: Long) {

    data class VoteRequest(
        /**
         * candidate’s term
         */
        override val term: Long,
        /**
         * candidate’s commit index
         */
        val leaderCommit: Long?
    ) : PaxosMessage(term)

    data class VoteResponse<C>(
        /**
         * currentTerm, for candidate to update itself
         */
        override val term: Long,
        /**
         * true indicates candidate received vote
         */
        val voteGranted: Boolean,
        /**
         * follower’s log entries after leaderCommit
         */
        val entries: List<LogEntry<C>>
    ) : PaxosMessage(term)
}