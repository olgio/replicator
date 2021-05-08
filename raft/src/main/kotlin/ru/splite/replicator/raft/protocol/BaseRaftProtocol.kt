package ru.splite.replicator.raft.protocol

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Mutex
import org.slf4j.LoggerFactory
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.raft.event.AppendEntryEvent
import ru.splite.replicator.raft.event.CommitEvent
import ru.splite.replicator.raft.event.IndexWithTerm
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.protocol.follower.AppendEntriesHandler
import ru.splite.replicator.raft.protocol.follower.VoteRequestHandler
import ru.splite.replicator.raft.protocol.leader.AppendEntriesSender
import ru.splite.replicator.raft.protocol.leader.CommandAppender
import ru.splite.replicator.raft.protocol.leader.CommitEntries
import ru.splite.replicator.raft.protocol.leader.VoteRequestSender
import ru.splite.replicator.raft.state.NodeStateStore
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.sender.MessageSender
import java.time.Instant

class BaseRaftProtocol(
    override val replicatedLogStore: ReplicatedLogStore,
    private val config: RaftProtocolConfig,
    private val localNodeStateStore: NodeStateStore,
    commitEntriesCondition: (LogEntry, Long) -> Boolean = { logEntry, currentTerm ->
        logEntry.term == currentTerm
    }
) : RaftProtocol {

    override val address: NodeIdentifier
        get() = config.address

    override val isLeader: Boolean
        get() = localNodeStateStore.getState().currentNodeType == NodeType.LEADER

    override val commitEventFlow: StateFlow<CommitEvent>
        get() = commitEntries.commitEventFlow

    override val appendEntryEventFlow: StateFlow<AppendEntryEvent>
        get() = commandAppender.appendEntryEventFlow

    private val leaderAliveMutableFlow: MutableStateFlow<Instant> = MutableStateFlow(Instant.now())
    override val leaderAliveEventFlow: StateFlow<Instant> = leaderAliveMutableFlow

    private val commitEntries =
        CommitEntries(localNodeStateStore, replicatedLogStore, commitEntriesCondition)

    private val stateMutex = Mutex()

    private val appendEntriesSender =
        AppendEntriesSender(config.address, localNodeStateStore, replicatedLogStore, stateMutex)

    private val appendEntriesHandler =
        AppendEntriesHandler(localNodeStateStore, replicatedLogStore, commitEntries, stateMutex)

    private val voteRequestSender =
        VoteRequestSender(config.address, localNodeStateStore, replicatedLogStore, stateMutex)

    private val voteRequestHandler = VoteRequestHandler(localNodeStateStore, replicatedLogStore, stateMutex)

    private val commandAppender = CommandAppender(localNodeStateStore, replicatedLogStore)

    override suspend fun sendVoteRequestsAsCandidate(messageSender: MessageSender<RaftMessage>): Boolean {
        val nodeIdentifiers = messageSender.getAllNodes().minus(address)
        return voteRequestSender.sendVoteRequestsAsCandidate(
            messageSender,
            nodeIdentifiers,
            config.leaderElectionQuorumSize
        )
    }

    override suspend fun commitLogEntriesIfLeader(messageSender: MessageSender<RaftMessage>): IndexWithTerm? =
        coroutineScope {
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
        val leaderIdentifier = localNodeStateStore.getState().leaderIdentifier
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