package ru.splite.replicator.raft

import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.Transport
import ru.splite.replicator.transport.TypedActor
import ru.splite.replicator.transport.sender.MessageSender

class RaftProtocolController(
    transport: Transport,
    config: RaftProtocolConfig,
    val protocol: RaftProtocol
) : TypedActor<RaftMessage>(config.address, transport, RaftMessage.serializer()) {

    private val messageSender = MessageSender(this, config.sendMessageTimeout)

    suspend fun sendVoteRequestsAsCandidate(): Boolean {
        return protocol.sendVoteRequestsAsCandidate(messageSender)
    }

    suspend fun commitLogEntriesIfLeader() {
        protocol.commitLogEntriesIfLeader(messageSender)
    }

    suspend fun sendAppendEntriesIfLeader() {
        return protocol.sendAppendEntriesIfLeader(messageSender)
    }

    suspend fun applyCommand(command: ByteArray): Long {
        return protocol.applyCommand(command)
    }

    override suspend fun receive(src: NodeIdentifier, payload: RaftMessage): RaftMessage {
        return when (payload) {
            is RaftMessage.VoteRequest -> protocol.handleVoteRequest(payload)
            is RaftMessage.AppendEntries -> protocol.handleAppendEntries(payload)
            else -> error("Message type ${payload.javaClass} is not supported")
        }
    }
}