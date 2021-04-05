package ru.splite.replicator.paxos

import kotlinx.coroutines.coroutineScope
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.protobuf.ProtoBuf
import org.slf4j.LoggerFactory
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.paxos.message.PaxosMessageReceiver
import ru.splite.replicator.paxos.state.PaxosLocalNodeState
import ru.splite.replicator.paxos.state.follower.VoteRequestHandler
import ru.splite.replicator.paxos.state.leader.VoteRequestSender
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.follower.AppendEntriesHandler
import ru.splite.replicator.raft.state.leader.AppendEntriesSender
import ru.splite.replicator.raft.state.leader.CommandAppender
import ru.splite.replicator.raft.state.leader.CommitEntries
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.Transport
import ru.splite.replicator.transport.TypedActor
import ru.splite.replicator.transport.sender.MessageSender

class PaxosProtocolController(
    override val replicatedLogStore: ReplicatedLogStore,
    transport: Transport,
    private val config: RaftProtocolConfig,
    private val localNodeState: PaxosLocalNodeState
) : PaxosMessageReceiver, PaxosProtocol,
    TypedActor<RaftMessage>(localNodeState.nodeIdentifier, transport, RaftMessage.serializer()) {

    private val messageSender = MessageSender(this, config.sendMessageTimeout)

    override val nodeIdentifier: NodeIdentifier
        get() = localNodeState.nodeIdentifier

    override val isLeader: Boolean
        get() = localNodeState.currentNodeType == NodeType.LEADER

    private val appendEntriesSender = AppendEntriesSender(localNodeState, replicatedLogStore)

    private val appendEntriesHandler = AppendEntriesHandler(localNodeState, replicatedLogStore)

    private val voteRequestSender = VoteRequestSender(localNodeState, replicatedLogStore)

    private val voteRequestHandler = VoteRequestHandler(localNodeState, replicatedLogStore)

    private val commitEntries = CommitEntries(localNodeState, replicatedLogStore) { logEntry, currentTerm ->
        true
    }

    private val commandAppender = CommandAppender(localNodeState, replicatedLogStore)

    override suspend fun sendVoteRequestsAsCandidate(): Boolean {
        return voteRequestSender.sendVoteRequestsAsCandidate(
            messageSender,
            config.leaderElectionQuorumSize
        )
    }

    override suspend fun commitLogEntriesIfLeader() = coroutineScope {
        commitEntries.commitLogEntriesIfLeader(transport, config.logReplicationQuorumSize)
    }

    override suspend fun sendAppendEntriesIfLeader() = coroutineScope {
        appendEntriesSender.sendAppendEntriesIfLeader(messageSender)
    }

    override fun applyCommand(command: ByteArray) {
        commandAppender.addCommand(command)
    }

    override suspend fun handleAppendEntries(request: RaftMessage.AppendEntries): RaftMessage.AppendEntriesResponse {
        LOGGER.debug("$nodeIdentifier :: received $request")
        val response = appendEntriesHandler.handleAppendEntries(request)
        if (response.entriesAppended) {
            LOGGER.debug("$nodeIdentifier :: entries successfully appended $response")
//            termIncrementerTimerTask.renew()
        }
        return response
    }

    override suspend fun handleVoteRequest(request: RaftMessage.PaxosVoteRequest): RaftMessage.PaxosVoteResponse {
        LOGGER.debug("$nodeIdentifier :: received $request")
        val response = voteRequestHandler.handleVoteRequest(request)
        if (response.voteGranted) {
            LOGGER.debug("$nodeIdentifier :: vote granted $response")
//            termIncrementerTimerTask.renew()
        }
        return response
    }


    override suspend fun receive(src: NodeIdentifier, payload: ByteArray): ByteArray {
        return when (val request: RaftMessage = ProtoBuf.decodeFromByteArray(payload)) {
            is RaftMessage.PaxosVoteRequest -> ProtoBuf.encodeToByteArray<RaftMessage>(handleVoteRequest(request))
            is RaftMessage.AppendEntries -> ProtoBuf.encodeToByteArray<RaftMessage>(handleAppendEntries(request))
            else -> error("Message type ${request.javaClass} is not supported")
        }
    }

    override suspend fun receive(src: NodeIdentifier, payload: RaftMessage): RaftMessage {
        return when (payload) {
            is RaftMessage.PaxosVoteRequest -> handleVoteRequest(payload)
            is RaftMessage.AppendEntries -> handleAppendEntries(payload)
            else -> error("Message type ${payload.javaClass} is not supported")
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}