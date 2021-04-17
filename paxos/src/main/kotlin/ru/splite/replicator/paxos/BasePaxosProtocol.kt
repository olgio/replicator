package ru.splite.replicator.paxos

import org.slf4j.LoggerFactory
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.paxos.state.PaxosLocalNodeState
import ru.splite.replicator.paxos.state.follower.VoteRequestHandler
import ru.splite.replicator.paxos.state.leader.VoteRequestSender
import ru.splite.replicator.raft.BaseRaftProtocol
import ru.splite.replicator.raft.RaftProtocol
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.transport.sender.MessageSender

class BasePaxosProtocol(
    replicatedLogStore: ReplicatedLogStore,
    private val config: RaftProtocolConfig,
    localNodeState: PaxosLocalNodeState
) : RaftProtocol by BaseRaftProtocol(replicatedLogStore, config, localNodeState, { _, _ -> true }), PaxosProtocol {

    private val voteRequestSender = VoteRequestSender(config.address, localNodeState, replicatedLogStore)

    private val voteRequestHandler = VoteRequestHandler(localNodeState, replicatedLogStore)

    override suspend fun sendVoteRequestsAsCandidate(messageSender: MessageSender<RaftMessage>): Boolean {
        val nodeIdentifiers = messageSender.getAllNodes().minus(address)
        return voteRequestSender.sendVoteRequestsAsCandidate(
            messageSender,
            nodeIdentifiers,
            config.leaderElectionQuorumSize
        )
    }

    override suspend fun handleVoteRequest(request: RaftMessage.PaxosVoteRequest): RaftMessage.PaxosVoteResponse {
        val response = voteRequestHandler.handleVoteRequest(request)
        if (response.voteGranted) {
            LOGGER.debug("$address :: vote granted $response")
        }
        return response
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}