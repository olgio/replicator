package ru.splite.replicator.raft.message

import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.log.LogEntry

sealed class RaftMessage(open val term: Long) {

    data class VoteRequest(
        override val term: Long,
        val candidateIdentifier: NodeIdentifier,
        val lastLogIndex: Long?,
        val lastLogTerm: Long?
    ) : RaftMessage(term)

    data class VoteResponse(
        override val term: Long,
        val voteGranted: Boolean
    ) : RaftMessage(term)

    data class AppendEntries<C>(
        override val term: Long,
        val leaderIdentifier: NodeIdentifier,
        val prevLogIndex: Long?,
        val prevLogTerm: Long?,
        val lastCommitIndex: Long?,
        val entries: List<LogEntry<C>>
    ) : RaftMessage(term)

    data class AppendEntriesResponse(
        override val term: Long,
        val entriesAppended: Boolean
    ) : RaftMessage(term)

}
