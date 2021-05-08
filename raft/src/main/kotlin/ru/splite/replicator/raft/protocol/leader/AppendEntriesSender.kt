package ru.splite.replicator.raft.protocol.leader

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
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

internal class AppendEntriesSender(
    private val nodeIdentifier: NodeIdentifier,
    private val localNodeStateStore: NodeStateStore,
    private val logStore: ReplicatedLogStore,
    private val stateMutex: Mutex
) {

    suspend fun sendAppendEntriesIfLeader(
        nodeIdentifiers: Collection<NodeIdentifier>,
        messageSender: MessageSender<RaftMessage>
    ) = coroutineScope {
        LOGGER.info("Sending AppendEntries (term ${localNodeStateStore.getState().currentTerm})")

        nodeIdentifiers.map { dstNodeIdentifier ->
            launch {
                sendAppendEntriesToNode(messageSender, dstNodeIdentifier)
            }
        }.forEach {
            it.join()
        }
    }

    private suspend fun sendAppendEntriesToNode(
        messageSender: MessageSender<RaftMessage>,
        dstNodeIdentifier: NodeIdentifier
    ) {
        val appendEntriesRequest: RaftMessage.AppendEntries = stateMutex.withLock {
            val currentState: ExternalNodeState = localNodeStateStore.getExternalNodeState(dstNodeIdentifier)
            val nextIndexPerNode: Long = currentState.nextIndex
            val newAppendEntriesRequest = buildAppendEntries(fromIndex = nextIndexPerNode)
            localNodeStateStore.setExternalNodeState(
                dstNodeIdentifier,
                ExternalNodeState(
                    nextIndex = nextIndexPerNode + newAppendEntriesRequest.entries.size,
                    matchIndex = currentState.matchIndex
                )
            )
            newAppendEntriesRequest
        }
        val appendEntriesResult = sendAppendEntriesMessage(messageSender, dstNodeIdentifier, appendEntriesRequest)

        stateMutex.withLock {
            val currentState = localNodeStateStore.getExternalNodeState(dstNodeIdentifier)
            if (appendEntriesResult == null) {
                val newNextIndex = maxOf(
                    currentState.nextIndex - appendEntriesRequest.entries.size,
                    currentState.matchIndex + 1L
                )
                localNodeStateStore.setExternalNodeState(
                    dstNodeIdentifier,
                    ExternalNodeState(
                        nextIndex = newNextIndex,
                        matchIndex = currentState.matchIndex
                    )
                )
                return
            }
            if (appendEntriesResult.entriesAppended) {
                val newMatchIndex = appendEntriesRequest.prevLogIndex + appendEntriesRequest.entries.size
                localNodeStateStore.setExternalNodeState(
                    dstNodeIdentifier,
                    ExternalNodeState(
                        nextIndex = currentState.nextIndex,
                        matchIndex = maxOf(newMatchIndex, currentState.matchIndex)
                    )
                )
            } else {
                if (appendEntriesResult.term > localNodeStateStore.getState().currentTerm) {
                    return
                }
                val newNextIndex = if (appendEntriesResult.conflictIndex >= 0L) {
                    appendEntriesResult.conflictIndex
                } else maxOf(0L, currentState.nextIndex - 1L)
                localNodeStateStore.setExternalNodeState(
                    dstNodeIdentifier,
                    ExternalNodeState(
                        nextIndex = maxOf(newNextIndex, currentState.matchIndex + 1L),
                        matchIndex = currentState.matchIndex
                    )
                )
            }
        }
    }

    private fun buildAppendEntries(fromIndex: Long): RaftMessage.AppendEntries =
        localNodeStateStore.getState().let { localNodeState ->
            if (fromIndex < 0) {
                error("fromIndex cannot be negative")
            }

            if (localNodeState.currentNodeType != NodeType.LEADER) {
                LOGGER.warn("Cannot send appendEntries because node is not leader. currentNodeType = ${localNodeState.currentNodeType}")
                error("Cannot send appendEntries because node is not leader")
            }

            val lastLogIndex: Long? = logStore.lastLogIndex()

            val lastCommitIndex: Long? = logStore.lastCommitIndex()

            if (lastLogIndex != null && fromIndex <= lastLogIndex) {

                val prevLogIndex: Long? = if (fromIndex > 0) fromIndex - 1 else null

                val prevLogTerm: Long? = prevLogIndex?.let { logStore.getLogEntryByIndex(it)!!.term }

                val entries = (fromIndex..lastLogIndex).map { logStore.getLogEntryByIndex(it)!! }.toList()

                return RaftMessage.AppendEntries(
                    term = localNodeState.currentTerm,
                    leaderIdentifier = nodeIdentifier,
                    prevLogIndex = prevLogIndex ?: -1,
                    prevLogTerm = prevLogTerm ?: -1,
                    lastCommitIndex = lastCommitIndex ?: -1,
                    entries = entries
                )
            }

            val lastLogTerm = lastLogIndex?.let { logStore.getLogEntryByIndex(it)!!.term }

            return RaftMessage.AppendEntries(
                term = localNodeState.currentTerm,
                leaderIdentifier = nodeIdentifier,
                prevLogIndex = lastLogIndex ?: -1,
                prevLogTerm = lastLogTerm ?: -1,
                lastCommitIndex = lastCommitIndex ?: -1,
                entries = emptyList()
            )
        }

    private suspend fun sendAppendEntriesMessage(
        messageSender: MessageSender<RaftMessage>,
        dstNodeIdentifier: NodeIdentifier,
        appendEntriesRequest: RaftMessage.AppendEntries
    ) = kotlin.runCatching {
        messageSender
            .sendOrThrow(dstNodeIdentifier, appendEntriesRequest) as RaftMessage.AppendEntriesResponse
    }.onFailure {
        LOGGER.error("Exception while sending AppendEntries to $dstNodeIdentifier", it)
    }.getOrNull()

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}