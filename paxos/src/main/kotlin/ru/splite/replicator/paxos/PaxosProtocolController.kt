package ru.splite.replicator.paxos

import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.raft.RaftProtocolController
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.Transport

class PaxosProtocolController(
    transport: Transport,
    config: RaftProtocolConfig,
    override val protocol: PaxosProtocol
) : RaftProtocolController(transport, config, protocol) {

    override suspend fun receive(src: NodeIdentifier, payload: RaftMessage): RaftMessage {
        return when (payload) {
            is RaftMessage.PaxosVoteRequest -> protocol.handleVoteRequest(payload)
            is RaftMessage.VoteRequest -> error("VoteRequest message is unsupported in Paxos")
            else -> super.receive(src, payload)
        }
    }
}