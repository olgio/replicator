package ru.splite.replicator.raft

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.protobuf.ProtoBuf
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.message.RaftMessageReceiver
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.RaftLocalNodeState
import ru.splite.replicator.raft.state.asMajority
import ru.splite.replicator.raft.state.follower.AppendEntriesHandler
import ru.splite.replicator.raft.state.follower.VoteRequestHandler
import ru.splite.replicator.raft.state.leader.*
import ru.splite.replicator.transport.Actor
import ru.splite.replicator.transport.Transport
import java.time.Instant

class RaftProtocolController(
    override val replicatedLogStore: ReplicatedLogStore,
    transport: Transport,
    private val localNodeState: RaftLocalNodeState,
    private val leaderElectionQuorumSize: Int = transport.nodes.size.asMajority(),
    private val logReplicationQuorumSize: Int = transport.nodes.size.asMajority()
) : RaftMessageReceiver, RaftProtocol, Actor(localNodeState.nodeIdentifier, transport) {

    init {
        val fullClusterSize = transport.nodes.size
        if (leaderElectionQuorumSize + logReplicationQuorumSize <= fullClusterSize) {
            error("Quorum requirement violation: $leaderElectionQuorumSize + $logReplicationQuorumSize >= $fullClusterSize")
        }
    }

    override val nodeIdentifier: NodeIdentifier
        get() = localNodeState.nodeIdentifier

    override val isLeader: Boolean
        get() = localNodeState.currentNodeType == NodeType.LEADER

    override val lastCommitIndexFlow: StateFlow<LastCommitEvent>
        get() = commitEntries.lastCommitIndexFlow

    private val leaderAliveMutableFlow: MutableStateFlow<Instant> = MutableStateFlow(Instant.now())
    override val leaderAliveFlow: StateFlow<Instant> = leaderAliveMutableFlow

    private val appendEntriesSender = AppendEntriesSender(localNodeState, replicatedLogStore)

    private val appendEntriesHandler = AppendEntriesHandler(localNodeState, replicatedLogStore)

    private val voteRequestSender = VoteRequestSender(localNodeState, replicatedLogStore)

    private val voteRequestHandler = VoteRequestHandler(localNodeState, replicatedLogStore)

    private val commitEntries = CommitEntries(localNodeState, replicatedLogStore) { logEntry, currentTerm ->
        logEntry.term == currentTerm
    }

    private val commandAppender = CommandAppender(localNodeState, replicatedLogStore)

    override suspend fun sendVoteRequestsAsCandidate(): Boolean {
        return voteRequestSender.sendVoteRequestsAsCandidate(
            this@RaftProtocolController,
            transport,
            leaderElectionQuorumSize
        )
    }

    override suspend fun commitLogEntriesIfLeader() = coroutineScope {
        commitEntries.commitLogEntriesIfLeader(transport, logReplicationQuorumSize)
    }

    override suspend fun sendAppendEntriesIfLeader() = coroutineScope {
        appendEntriesSender.sendAppendEntriesIfLeader(this@RaftProtocolController, transport)
    }

    override suspend fun applyCommand(command: ByteArray): Long {
        return commandAppender.addCommand(command)
    }

    override suspend fun handleAppendEntries(request: RaftMessage.AppendEntries): RaftMessage.AppendEntriesResponse {
        LOGGER.debug("$nodeIdentifier :: received $request")
        val response = appendEntriesHandler.handleAppendEntries(request)
        if (response.entriesAppended) {
            LOGGER.debug("$nodeIdentifier :: entries successfully appended $response")
            leaderAliveMutableFlow.tryEmit(Instant.now())
        }
        return response
    }

    override suspend fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse {
        LOGGER.debug("$nodeIdentifier :: received $request")
        val response = voteRequestHandler.handleVoteRequest(request)
        if (response.voteGranted) {
            LOGGER.debug("$nodeIdentifier :: vote granted $response")
            leaderAliveMutableFlow.tryEmit(Instant.now())
        }
        return response
    }

    override suspend fun receive(src: NodeIdentifier, payload: ByteArray): ByteArray {
        return when (val request: RaftMessage = ProtoBuf.decodeFromByteArray(payload)) {
            is RaftMessage.VoteRequest -> ProtoBuf.encodeToByteArray<RaftMessage>(handleVoteRequest(request))
            is RaftMessage.AppendEntries -> ProtoBuf.encodeToByteArray<RaftMessage>(handleAppendEntries(request))
            else -> error("Message type ${request.javaClass} is not supported")
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}