package ru.splite.replicator.paxos

import org.slf4j.LoggerFactory
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.raft.event.IndexWithTerm
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.Transport
import ru.splite.replicator.transport.TypedActor
import ru.splite.replicator.transport.sender.MessageSender

class PaxosProtocolController(
    transport: Transport,
    config: RaftProtocolConfig,
    val protocol: PaxosProtocol
) : TypedActor<RaftMessage>(config.address, transport, RaftMessage.serializer()) {

    private val messageSender = MessageSender(this, config.sendMessageTimeout)

    override suspend fun receive(src: NodeIdentifier, payload: RaftMessage): RaftMessage {
        return when (payload) {
            is RaftMessage.PaxosVoteRequest -> protocol.handleVoteRequest(payload)
            is RaftMessage.AppendEntries -> protocol.handleAppendEntries(payload)
            else -> error("Message type ${payload.javaClass} is not supported")
        }
    }

    suspend fun sendVoteRequestsAsCandidate(): Boolean {
        return protocol.sendVoteRequestsAsCandidate(messageSender)
    }

    suspend fun commitLogEntriesIfLeader(): IndexWithTerm? {
        return protocol.commitLogEntriesIfLeader(messageSender)
    }

    suspend fun sendAppendEntriesIfLeader() {
        return protocol.sendAppendEntriesIfLeader(messageSender)
    }

    suspend fun applyCommand(command: ByteArray): IndexWithTerm {
        return protocol.applyCommand(command)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}