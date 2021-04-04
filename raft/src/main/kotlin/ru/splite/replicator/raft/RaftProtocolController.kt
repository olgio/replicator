package ru.splite.replicator.raft

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.slf4j.LoggerFactory
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.message.RaftMessageReceiver
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.RaftLocalNodeState
import ru.splite.replicator.raft.state.follower.AppendEntriesHandler
import ru.splite.replicator.raft.state.follower.VoteRequestHandler
import ru.splite.replicator.raft.state.leader.*
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.Transport
import ru.splite.replicator.transport.TypedActor
import ru.splite.replicator.transport.sender.MessageSender
import java.time.Instant

class RaftProtocolController(
    override val replicatedLogStore: ReplicatedLogStore,
    transport: Transport,
    private val config: RaftProtocolConfig,
    private val localNodeState: RaftLocalNodeState
) : RaftMessageReceiver, RaftProtocol,
    TypedActor<RaftMessage>(localNodeState.nodeIdentifier, transport, RaftMessage.serializer()) {

    private val messageSender = MessageSender(this, config.sendMessageTimeout)

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

    override suspend fun receive(src: NodeIdentifier, payload: RaftMessage): RaftMessage {
        return when (payload) {
            is RaftMessage.VoteRequest -> handleVoteRequest(payload)
            is RaftMessage.AppendEntries -> handleAppendEntries(payload)
            else -> error("Message type ${payload.javaClass} is not supported")
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}