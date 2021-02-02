package ru.splite.replicator.paxos.state.leader

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.ClusterTopology
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.paxos.message.PaxosMessage
import ru.splite.replicator.paxos.message.PaxosMessageReceiver
import ru.splite.replicator.paxos.state.PaxosLocalNodeState
import ru.splite.replicator.raft.state.ExternalNodeState
import ru.splite.replicator.raft.state.NodeType

class VoteRequestSender<C>(
    private val localNodeState: PaxosLocalNodeState<C>,
    private val logStore: ReplicatedLogStore<C>
) {

    suspend fun sendVoteRequestsAsCandidate(
        clusterTopology: ClusterTopology<PaxosMessageReceiver<C>>,
        majority: Int
    ): Boolean = coroutineScope {
        val clusterNodeIdentifiers = clusterTopology.nodes.minus(localNodeState.nodeIdentifier)

        if (clusterNodeIdentifiers.isEmpty()) {
            error("Cluster cannot be empty")
        }
        val nextTerm: Long = calculateNextTerm(clusterTopology.nodes.size.toLong())

        val lastCommitIndex: Long? = logStore.lastCommitIndex()
        val voteRequest: PaxosMessage.VoteRequest = becomeCandidate(nextTerm, lastCommitIndex)

        val voteResponses: List<PaxosMessage.VoteResponse<C>> = clusterNodeIdentifiers.map { dstNodeIdentifier ->
            async {
                kotlin.runCatching {
                    val voteResponse: PaxosMessage.VoteResponse<C> = withTimeout(1000) {
                        clusterTopology[dstNodeIdentifier].handleVoteRequest(voteRequest)
                    }
                    voteResponse
                }.getOrNull()
            }
        }.mapNotNull {
            it.await()
        }.filter {
            it.voteGranted
        }

        val voteGrantedCount: Int = voteResponses.size + 1

        LOGGER.info("${localNodeState.nodeIdentifier} :: VoteResult for term ${localNodeState.currentTerm}: ${voteGrantedCount}/${clusterTopology.nodes.size} (majority = ${majority})")
        if (voteGrantedCount >= majority) {
            handleVoteResponsesIfMajority(nextTerm, voteResponses)
            becomeLeader()
            reinitializeExternalNodeStates(clusterTopology.nodes)
            true
        } else {
            false
        }
    }

    private fun becomeCandidate(nextTerm: Long, lastCommitIndex: Long?): PaxosMessage.VoteRequest {
        LOGGER.debug("${localNodeState.nodeIdentifier} :: state transition ${localNodeState.currentNodeType} (term ${localNodeState.currentTerm}) -> CANDIDATE")
        localNodeState.currentTerm = nextTerm
        localNodeState.currentNodeType = NodeType.CANDIDATE

        return PaxosMessage.VoteRequest(
            term = localNodeState.currentTerm,
            leaderCommit = lastCommitIndex
        )
    }

    private fun becomeLeader() {
        LOGGER.debug("${localNodeState.nodeIdentifier} :: state transition ${localNodeState.currentNodeType} -> LEADER (term ${localNodeState.currentTerm})")
        localNodeState.leaderIdentifier = localNodeState.nodeIdentifier
        localNodeState.currentNodeType = NodeType.LEADER
    }

    private fun reinitializeExternalNodeStates(clusterNodeIdentifiers: Collection<NodeIdentifier>) {
        val nextIndex: Long = logStore.lastCommitIndex()?.plus(1) ?: 0
        clusterNodeIdentifiers.forEach { dstNodeIdentifier ->
            localNodeState.externalNodeStates[dstNodeIdentifier] =
                ExternalNodeState(nextIndex = nextIndex, matchIndex = -1)
        }
    }

    private fun handleVoteResponsesIfMajority(nextTerm: Long, voteResponses: List<PaxosMessage.VoteResponse<C>>) {
        val firstUncommittedIndex: Long = logStore.lastCommitIndex()?.plus(1) ?: 0

        val entries: MutableList<LogEntry<C>> = generateSequence(firstUncommittedIndex) {
            it + 1
        }.map {
            logStore.getLogEntryByIndex(it)
        }.takeWhile {
            it != null
        }.filterNotNull().toMutableList()

        voteResponses.forEach { voteResponse ->
            voteResponse.entries.forEachIndexed { index, logEntryFromFollower ->
                if (index > entries.lastIndex) {
                    entries.add(logEntryFromFollower)
                } else {
                    val currentLogEntry = entries[index]
                    if (logEntryFromFollower.term > currentLogEntry.term) {
                        entries[index] = logEntryFromFollower
                    }
                }
            }
        }
        entries.forEachIndexed { index, logEntry ->
            LOGGER.debug("${localNodeState.nodeIdentifier} :: logEntry with index ${firstUncommittedIndex + index} set to ${logEntry.command} with term ${nextTerm}")
            logStore.setLogEntry(firstUncommittedIndex + index, LogEntry(nextTerm, logEntry.command))
        }
    }

    private fun calculateNextTerm(fullClusterSize: Long): Long {
        val currentTerm: Long = localNodeState.currentTerm
        val currentRoundDelta = if (currentTerm % fullClusterSize >= localNodeState.uniqueNodeIdentifier) 1L else 0L
        val currentRound = currentTerm / fullClusterSize + currentRoundDelta
        return currentRound * fullClusterSize + localNodeState.uniqueNodeIdentifier
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}