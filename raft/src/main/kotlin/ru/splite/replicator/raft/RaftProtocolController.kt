package ru.splite.replicator.raft

import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.ClusterTopology
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.raft.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.message.RaftMessageReceiver
import ru.splite.replicator.raft.state.LocalNodeState
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.follower.AppendEntriesHandler
import ru.splite.replicator.raft.state.follower.VoteRequestHandler
import ru.splite.replicator.raft.state.leader.AppendEntriesSender
import ru.splite.replicator.raft.state.leader.CommandAppender
import ru.splite.replicator.raft.state.leader.CommitEntries
import ru.splite.replicator.raft.state.leader.VoteRequestSender

class RaftProtocolController<C>(
    override val replicatedLogStore: ReplicatedLogStore<C>,
    private val clusterTopology: ClusterTopology<RaftMessageReceiver<C>>,
    private val localNodeState: LocalNodeState
) : RaftMessageReceiver<C>, RaftProtocol<C> {

    override val nodeIdentifier: NodeIdentifier
        get() = localNodeState.nodeIdentifier

    override val isLeader: Boolean
        get() = localNodeState.currentNodeType == NodeType.LEADER

    private val majority: Int
        get() = Math.floorDiv(clusterTopology.nodes.size, 2)

    private val appendEntriesSender = AppendEntriesSender(localNodeState, replicatedLogStore)

    private val appendEntriesHandler = AppendEntriesHandler(localNodeState, replicatedLogStore)

    private val voteRequestSender = VoteRequestSender(localNodeState, replicatedLogStore)

    private val voteRequestHandler = VoteRequestHandler(localNodeState, replicatedLogStore)

    private val commitEntries = CommitEntries(localNodeState, replicatedLogStore)

    private val commandAppender = CommandAppender(localNodeState, replicatedLogStore)

    override suspend fun sendVoteRequestsAsCandidate(): Boolean {
        return voteRequestSender.sendVoteRequestsAsCandidate(clusterTopology, majority)
    }

    override suspend fun commitLogEntriesIfLeader() = coroutineScope {
        commitEntries.commitLogEntriesIfLeader(clusterTopology.nodes, majority)
    }

    override suspend fun sendAppendEntriesIfLeader() = coroutineScope {
        appendEntriesSender.sendAppendEntriesIfLeader(clusterTopology)
    }

    override suspend fun handleAppendEntries(request: RaftMessage.AppendEntries<C>): RaftMessage.AppendEntriesResponse {
        LOGGER.debug("$nodeIdentifier :: received $request")
        val response = appendEntriesHandler.handleAppendEntries(request)
        if (response.entriesAppended) {
            LOGGER.debug("$nodeIdentifier :: entries successfully appended $response")
//            termIncrementerTimerTask.renew()
        }
        return response
    }

    override suspend fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse {
        LOGGER.debug("$nodeIdentifier :: received $request")
        val response = voteRequestHandler.handleVoteRequest(request)
        if (response.voteGranted) {
            LOGGER.debug("$nodeIdentifier :: vote granted $response")
//            termIncrementerTimerTask.renew()
        }
        return response
    }

    override fun applyCommand(command: C) {
        commandAppender.addCommand(command)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}