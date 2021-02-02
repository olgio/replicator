package ru.splite.replicator.paxos.message

interface PaxosVoteRequestMessageReceiver<C> {

    suspend fun handleVoteRequest(request: PaxosMessage.VoteRequest): PaxosMessage.VoteResponse<C>
}