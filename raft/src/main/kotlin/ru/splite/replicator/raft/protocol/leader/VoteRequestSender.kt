package ru.splite.replicator.raft.protocol.leader

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.ExternalNodeState
import ru.splite.replicator.raft.state.NodeStateStore
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.sender.MessageSender

internal class VoteRequestSender(
    private val nodeIdentifier: NodeIdentifier,
    private val localNodeStateStore: NodeStateStore,
    private val logStore: ReplicatedLogStore,
    private val stateMutex: Mutex
) {

    suspend fun sendVoteRequestsAsCandidate(
        messageSender: MessageSender<RaftMessage>,
        nodeIdentifiers: Collection<NodeIdentifier>,
        quorumSize: Int
    ): Boolean = coroutineScope {
        val voteRequest: RaftMessage.VoteRequest = stateMutex.withLock {
            becomeCandidate()
        }

        val voteGrantedCount = nodeIdentifiers
            .map {
                async {
                    val result = kotlin.runCatching {
                        messageSender.sendOrThrow(it, voteRequest) as RaftMessage.VoteResponse
                    }
                    if (result.isFailure) {
                        LOGGER.error("Exception while sending VoteRequest to $it", result.exceptionOrNull())
                    }
                    result.getOrNull()
                }
            }.mapNotNull {
                it.await()
            }.filter {
                it.voteGranted
            }.count() + 1

        LOGGER.info("VoteResult for term ${voteRequest.term}: ${voteGrantedCount}/${messageSender.getAllNodes().size} (quorum = ${quorumSize})")
        if (voteGrantedCount >= quorumSize) {
            stateMutex.withLock {
                becomeLeader()
                reinitializeExternalNodeStates(messageSender.getAllNodes())
            }
            true
        } else {
            false
        }
    }

    private suspend fun becomeCandidate(): RaftMessage.VoteRequest =
        localNodeStateStore.getState().let { localNodeState ->
            LOGGER.debug("State transition ${localNodeState.currentNodeType} (term ${localNodeState.currentTerm}) -> CANDIDATE")
            val newTerm = localNodeState.currentTerm + 1
            localNodeStateStore.setState(
                localNodeState.copy(
                    currentTerm = newTerm,
                    lastVotedLeaderIdentifier = nodeIdentifier,
                    currentNodeType = NodeType.CANDIDATE
                )
            )

            val lastLogIndex: Long? = logStore.lastLogIndex()
            val lastLogTerm: Long? = lastLogIndex?.let { logStore.getLogEntryByIndex(it)!!.term }

            return RaftMessage.VoteRequest(
                term = newTerm, candidateIdentifier = nodeIdentifier,
                lastLogIndex = lastLogIndex ?: -1, lastLogTerm = lastLogTerm ?: -1
            )
        }

    private suspend fun becomeLeader() = localNodeStateStore.getState().let { localNodeState ->
        LOGGER.debug("State transition ${localNodeState.currentNodeType} -> LEADER (term ${localNodeState.currentTerm})")
        localNodeStateStore.setState(
            localNodeState.copy(
                currentNodeType = NodeType.LEADER,
                lastVotedLeaderIdentifier = null,
                leaderIdentifier = nodeIdentifier
            )
        )
    }

    private suspend fun reinitializeExternalNodeStates(clusterNodeIdentifiers: Collection<NodeIdentifier>) {
        val lastLogIndex = logStore.lastLogIndex()?.plus(1) ?: 0
        clusterNodeIdentifiers.forEach { dstNodeIdentifier ->
            localNodeStateStore.setExternalNodeState(
                dstNodeIdentifier,
                ExternalNodeState(nextIndex = lastLogIndex, matchIndex = -1)
            )
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}