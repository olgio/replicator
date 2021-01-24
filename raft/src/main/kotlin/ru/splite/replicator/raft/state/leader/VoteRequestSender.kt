package ru.splite.replicator.raft.state.leader

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.ClusterTopology
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.raft.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.message.RaftMessageReceiver
import ru.splite.replicator.raft.state.ExternalNodeState
import ru.splite.replicator.raft.state.LocalNodeState
import ru.splite.replicator.raft.state.NodeType

class VoteRequestSender(
    private val localNodeState: LocalNodeState,
    private val logStore: ReplicatedLogStore<*>
) {

    suspend fun sendVoteRequestsAsCandidate(
        clusterTopology: ClusterTopology<RaftMessageReceiver<*>>,
        majority: Int
    ): Boolean = coroutineScope {
        val clusterNodeIdentifiers = clusterTopology.nodes.minus(localNodeState.nodeIdentifier)

        if (clusterNodeIdentifiers.isEmpty()) {
            error("Cluster cannot be empty")
        }

        val voteRequest: RaftMessage.VoteRequest = becomeCandidate()
        val voteGrantedCount = clusterNodeIdentifiers.map { dstNodeIdentifier ->
            async {
                kotlin.runCatching {
                    val voteResponse: RaftMessage.VoteResponse = withTimeout(1000) {
                        clusterTopology[dstNodeIdentifier].handleVoteRequest(voteRequest)
                    }
                    voteResponse
                }.getOrNull()
            }
        }.mapNotNull {
            it.await()
        }.filter {
            it.voteGranted
        }.size + 1

        LOGGER.info("${localNodeState.nodeIdentifier} :: VoteResult for term ${localNodeState.currentTerm}: ${voteGrantedCount}/${clusterTopology.nodes.size} (majority = ${majority})")
        if (voteGrantedCount >= majority) {
            becomeLeader()
            reinitializeExternalNodeStates(clusterTopology.nodes)
            true
        } else {
            false
        }
    }

    private fun becomeCandidate(): RaftMessage.VoteRequest {
        LOGGER.debug("${localNodeState.nodeIdentifier} :: state transition ${localNodeState.currentNodeType} (term ${localNodeState.currentTerm}) -> CANDIDATE")
        localNodeState.currentTerm = localNodeState.currentTerm + 1
        localNodeState.lastVotedLeaderIdentifier = localNodeState.nodeIdentifier
        localNodeState.currentNodeType = NodeType.CANDIDATE

        val lastLogIndex: Long? = logStore.lastLogIndex()
        val lastLogTerm: Long? = lastLogIndex?.let { logStore.getLogEntryByIndex(it)!!.term }

        return RaftMessage.VoteRequest(
            term = localNodeState.currentTerm, candidateIdentifier = localNodeState.nodeIdentifier,
            lastLogIndex = lastLogIndex, lastLogTerm = lastLogTerm
        )
    }

    private fun becomeLeader() {
        LOGGER.debug("${localNodeState.nodeIdentifier} :: state transition ${localNodeState.currentNodeType} -> LEADER (term ${localNodeState.currentTerm})")
        localNodeState.lastVotedLeaderIdentifier = null
        localNodeState.leaderIdentifier = localNodeState.nodeIdentifier
        localNodeState.currentNodeType = NodeType.LEADER
    }

    private fun reinitializeExternalNodeStates(clusterNodeIdentifiers: Collection<NodeIdentifier>) {
        val lastLogIndex = logStore.lastLogIndex()?.plus(1) ?: 0
        clusterNodeIdentifiers.forEach { dstNodeIdentifier ->
            localNodeState.externalNodeStates[dstNodeIdentifier] =
                ExternalNodeState(nextIndex = lastLogIndex, matchIndex = -1)
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}