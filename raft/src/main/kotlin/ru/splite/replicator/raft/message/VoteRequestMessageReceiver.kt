package ru.splite.replicator.raft.message

interface VoteRequestMessageReceiver {

    suspend fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse
}