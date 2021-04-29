package ru.splite.replicator.paxos.protocol

import org.slf4j.LoggerFactory
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.paxos.protocol.follower.VoteRequestHandler
import ru.splite.replicator.paxos.protocol.leader.VoteRequestSender
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.protocol.BaseRaftProtocol
import ru.splite.replicator.raft.protocol.RaftProtocol
import ru.splite.replicator.raft.state.NodeStateStore
import ru.splite.replicator.transport.sender.MessageSender

class BasePaxosProtocol(
    replicatedLogStore: ReplicatedLogStore,
    private val config: RaftProtocolConfig,
    localNodeStateStore: NodeStateStore
) : RaftProtocol by BaseRaftProtocol(
    replicatedLogStore, config, localNodeStateStore, { _, _ -> true }), PaxosProtocol {

    private val voteRequestSender =
        VoteRequestSender(config.address, config.processId, localNodeStateStore, replicatedLogStore)

    private val voteRequestHandler = VoteRequestHandler(localNodeStateStore, replicatedLogStore)

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