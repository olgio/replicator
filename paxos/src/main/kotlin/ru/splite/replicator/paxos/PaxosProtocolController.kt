package ru.splite.replicator.paxos

import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.ClusterTopology
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.paxos.message.PaxosMessage
import ru.splite.replicator.paxos.message.PaxosMessageReceiver
import ru.splite.replicator.paxos.state.PaxosLocalNodeState
import ru.splite.replicator.paxos.state.follower.VoteRequestHandler
import ru.splite.replicator.paxos.state.leader.VoteRequestSender
import ru.splite.replicator.raft.asMajority
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.follower.AppendEntriesHandler
import ru.splite.replicator.raft.state.leader.AppendEntriesSender
import ru.splite.replicator.raft.state.leader.CommandAppender
import ru.splite.replicator.raft.state.leader.CommitEntries

class PaxosProtocolController(
    override val replicatedLogStore: ReplicatedLogStore,
    private val clusterTopology: ClusterTopology<PaxosMessageReceiver>,
    private val localNodeState: PaxosLocalNodeState,
    private val leaderElectionQuorumSize: Int = clusterTopology.nodes.size.asMajority(),
    private val logReplicationQuorumSize: Int = clusterTopology.nodes.size.asMajority()
) : PaxosMessageReceiver, PaxosProtocol {

    init {
        val fullClusterSize = clusterTopology.nodes.size
        if (leaderElectionQuorumSize + logReplicationQuorumSize <= fullClusterSize) {
            error("Quorum requirement violation: $leaderElectionQuorumSize + $logReplicationQuorumSize >= $fullClusterSize")
        }
    }

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
        return voteRequestSender.sendVoteRequestsAsCandidate(clusterTopology, leaderElectionQuorumSize)
    }

    override suspend fun commitLogEntriesIfLeader() = coroutineScope {
        commitEntries.commitLogEntriesIfLeader(clusterTopology, logReplicationQuorumSize)
    }

    override suspend fun sendAppendEntriesIfLeader() = coroutineScope {
        appendEntriesSender.sendAppendEntriesIfLeader(clusterTopology)
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

    override suspend fun handleVoteRequest(request: PaxosMessage.VoteRequest): PaxosMessage.VoteResponse {
        LOGGER.debug("$nodeIdentifier :: received $request")
        val response = voteRequestHandler.handleVoteRequest(request)
        if (response.voteGranted) {
            LOGGER.debug("$nodeIdentifier :: vote granted $response")
//            termIncrementerTimerTask.renew()
        }
        return response
    }

    override fun applyCommand(command: ByteArray) {
        commandAppender.addCommand(command)
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}