package ru.splite.replicator.paxos.message

import ru.splite.replicator.raft.message.RaftMessage

interface PaxosVoteRequestMessageReceiver {

    suspend fun handleVoteRequest(request: RaftMessage.PaxosVoteRequest): RaftMessage.PaxosVoteResponse
}