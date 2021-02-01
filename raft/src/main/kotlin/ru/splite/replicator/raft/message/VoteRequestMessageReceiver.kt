package ru.splite.replicator.raft.message

interface VoteRequestMessageReceiver<C> {

    suspend fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse
}