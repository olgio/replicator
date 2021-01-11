package ru.splite.replicator.raft.message

interface RaftMessageReceiver<C> {

    suspend fun handleAppendEntries(request: RaftMessage.AppendEntries<C>): RaftMessage.AppendEntriesResponse

    suspend fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse
}