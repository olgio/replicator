package ru.splite.replicator.paxos.message

interface PaxosVoteRequestMessageReceiver {

    suspend fun handleVoteRequest(request: PaxosMessage.VoteRequest): PaxosMessage.VoteResponse
}