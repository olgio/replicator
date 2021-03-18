package ru.splite.replicator.paxos.state.leader

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.paxos.state.PaxosLocalNodeState
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.ExternalNodeState
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.transport.Actor
import ru.splite.replicator.transport.Transport
import ru.splite.replicator.transport.sendProto

class VoteRequestSender(
    private val localNodeState: PaxosLocalNodeState,
    private val logStore: ReplicatedLogStore
) {

    suspend fun sendVoteRequestsAsCandidate(
        actor: Actor,
        transport: Transport,
        majority: Int
    ): Boolean = coroutineScope {
        val clusterNodeIdentifiers = transport.nodes.minus(localNodeState.nodeIdentifier)

        if (clusterNodeIdentifiers.isEmpty()) {
            error("Cluster cannot be empty")
        }
        val nextTerm: Long = calculateNextTerm(transport.nodes.size.toLong())

        val lastCommitIndex: Long? = logStore.lastCommitIndex()
        val voteRequest: RaftMessage.PaxosVoteRequest = becomeCandidate(nextTerm, lastCommitIndex)

        val voteResponses: List<RaftMessage.PaxosVoteResponse> = clusterNodeIdentifiers.map { dstNodeIdentifier ->
            async {
                val result = kotlin.runCatching {
                    val voteResponse: RaftMessage.PaxosVoteResponse = withTimeout(1000) {
                        actor.sendProto<RaftMessage, RaftMessage>(
                            dstNodeIdentifier,
                            voteRequest
                        ) as RaftMessage.PaxosVoteResponse
                    }
                    voteResponse
                }
                if (result.isFailure) {
                    LOGGER.error("Exception while sending VoteRequest to $dstNodeIdentifier", result.exceptionOrNull())
                }
                result.getOrNull()
            }
        }.mapNotNull {
            it.await()
        }.filter {
            it.voteGranted
        }

        val voteGrantedCount: Int = voteResponses.size + 1

        LOGGER.info("${localNodeState.nodeIdentifier} :: VoteResult for term ${localNodeState.currentTerm}: ${voteGrantedCount}/${transport.nodes.size} (majority = ${majority})")
        if (voteGrantedCount >= majority) {
            handleVoteResponsesIfMajority(nextTerm, voteResponses)
            becomeLeader()
            reinitializeExternalNodeStates(transport.nodes)
            true
        } else {
            false
        }
    }

    private fun becomeCandidate(nextTerm: Long, lastCommitIndex: Long?): RaftMessage.PaxosVoteRequest {
        LOGGER.debug("${localNodeState.nodeIdentifier} :: state transition ${localNodeState.currentNodeType} (term ${localNodeState.currentTerm}) -> CANDIDATE")
        localNodeState.currentTerm = nextTerm
        localNodeState.currentNodeType = NodeType.CANDIDATE

        return RaftMessage.PaxosVoteRequest(
            term = localNodeState.currentTerm,
            leaderCommit = lastCommitIndex ?: -1
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

    private fun handleVoteResponsesIfMajority(nextTerm: Long, voteResponses: List<RaftMessage.PaxosVoteResponse>) {
        val firstUncommittedIndex: Long = logStore.lastCommitIndex()?.plus(1) ?: 0

        val entries: MutableList<LogEntry> = generateSequence(firstUncommittedIndex) {
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
            LOGGER.debug("${localNodeState.nodeIdentifier} :: logEntry with index ${firstUncommittedIndex + index} set to ${logEntry.command} with term $nextTerm")
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