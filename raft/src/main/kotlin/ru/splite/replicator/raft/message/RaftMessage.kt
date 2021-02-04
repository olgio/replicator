package ru.splite.replicator.raft.message

import kotlinx.serialization.Serializable
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.log.LogEntry

@Serializable
sealed class RaftMessage() {

    @Serializable
    data class VoteRequest(
        val term: Long,
        val candidateIdentifier: NodeIdentifier,
        val lastLogIndex: Long?,
        val lastLogTerm: Long?
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
        val prevLogIndex: Long?,
        val prevLogTerm: Long?,
        val lastCommitIndex: Long?,
        val entries: List<LogEntry>
    ) : RaftMessage()

    @Serializable
    data class AppendEntriesResponse(
        val term: Long,
        val entriesAppended: Boolean
    ) : RaftMessage()

}
