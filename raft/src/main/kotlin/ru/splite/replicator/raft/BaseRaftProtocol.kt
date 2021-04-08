package ru.splite.replicator.raft

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.slf4j.LoggerFactory
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.event.AppendEntryEvent
import ru.splite.replicator.raft.event.CommitEvent
import ru.splite.replicator.raft.event.IndexWithTerm
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.RaftLocalNodeState
import ru.splite.replicator.raft.state.follower.AppendEntriesHandler
import ru.splite.replicator.raft.state.follower.VoteRequestHandler
import ru.splite.replicator.raft.state.leader.AppendEntriesSender
import ru.splite.replicator.raft.state.leader.CommandAppender
import ru.splite.replicator.raft.state.leader.CommitEntries
import ru.splite.replicator.raft.state.leader.VoteRequestSender
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.sender.MessageSender
import java.time.Instant

class BaseRaftProtocol(
    override val replicatedLogStore: ReplicatedLogStore,
    private val config: RaftProtocolConfig,
    private val localNodeState: RaftLocalNodeState
) : RaftProtocol {

    override val address: NodeIdentifier
        get() = config.address

    override val isLeader: Boolean
        get() = localNodeState.currentNodeType == NodeType.LEADER

    override val commitEventFlow: StateFlow<CommitEvent>
        get() = commitEntries.commitEventFlow

    override val appendEntryEventFlow: StateFlow<AppendEntryEvent>
        get() = commandAppender.appendEntryEventFlow

    private val leaderAliveMutableFlow: MutableStateFlow<Instant> = MutableStateFlow(Instant.now())
    override val leaderAliveEventFlow: StateFlow<Instant> = leaderAliveMutableFlow

    private val commitEntries =
        CommitEntries(localNodeState, replicatedLogStore) { logEntry, currentTerm ->
            logEntry.term == currentTerm
        }

    private val appendEntriesSender = AppendEntriesSender(config.address, localNodeState, replicatedLogStore)

    private val appendEntriesHandler = AppendEntriesHandler(localNodeState, replicatedLogStore, commitEntries)

    private val voteRequestSender = VoteRequestSender(config.address, localNodeState, replicatedLogStore)

    private val voteRequestHandler = VoteRequestHandler(localNodeState, replicatedLogStore)

    private val commandAppender = CommandAppender(localNodeState, replicatedLogStore)

    override suspend fun sendVoteRequestsAsCandidate(messageSender: MessageSender<RaftMessage>): Boolean {
        val nodeIdentifiers = messageSender.getAllNodes().minus(address)
        return voteRequestSender.sendVoteRequestsAsCandidate(
            messageSender,
            nodeIdentifiers,
            config.leaderElectionQuorumSize
        )
    }

    override suspend fun commitLogEntriesIfLeader(messageSender: MessageSender<RaftMessage>) = coroutineScope {
        val nodeIdentifiers = messageSender.getAllNodes().minus(address)
        commitEntries.commitLogEntriesIfLeader(nodeIdentifiers, config.logReplicationQuorumSize)
    }

    override suspend fun sendAppendEntriesIfLeader(messageSender: MessageSender<RaftMessage>) = coroutineScope {
        val nodeIdentifiers = messageSender.getAllNodes().minus(address)
        appendEntriesSender.sendAppendEntriesIfLeader(nodeIdentifiers, messageSender)
    }

    override suspend fun appendCommand(command: ByteArray): IndexWithTerm {
        return commandAppender.addCommand(command)
    }

    override suspend fun redirectAndAppendCommand(
        messageSender: MessageSender<RaftMessage>,
        command: ByteArray
    ): IndexWithTerm {
        val redirectMessage = RaftMessage.RedirectRequest(command = command)
        val leaderIdentifier = localNodeState.leaderIdentifier
            ?: error("Cannot determine leader to redirect")
        LOGGER.debug("Redirecting command to $leaderIdentifier")
        val redirectResponse =
            messageSender.sendOrThrow(leaderIdentifier, redirectMessage) as RaftMessage.RedirectResponse
        LOGGER.debug("Command successfully redirected to $leaderIdentifier: $redirectResponse")
        return IndexWithTerm(index = redirectResponse.index, term = redirectResponse.term)
    }

    override suspend fun handleAppendEntries(request: RaftMessage.AppendEntries): RaftMessage.AppendEntriesResponse {
        val response = appendEntriesHandler.handleAppendEntries(request)
        if (response.entriesAppended) {
            LOGGER.debug("$address :: entries successfully appended $response")
            leaderAliveMutableFlow.tryEmit(Instant.now())
        }
        return response
    }

    override suspend fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse {
        val response = voteRequestHandler.handleVoteRequest(request)
        if (response.voteGranted) {
            LOGGER.debug("$address :: vote granted $response")
            leaderAliveMutableFlow.tryEmit(Instant.now())
        }
        return response
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}