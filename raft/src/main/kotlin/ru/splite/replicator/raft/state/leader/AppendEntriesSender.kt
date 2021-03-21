package ru.splite.replicator.raft.state.leader

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.RaftLocalNodeState
import ru.splite.replicator.transport.sender.MessageSender

class AppendEntriesSender(
    private val localNodeState: RaftLocalNodeState,
    private val logStore: ReplicatedLogStore
) {

    private data class AppendEntriesResult(
        val dstNodeIdentifier: NodeIdentifier,
        val matchIndex: Long,
        val isSuccess: Boolean
    )

    suspend fun sendAppendEntriesIfLeader(messageSender: MessageSender<RaftMessage>) =
        coroutineScope {
            LOGGER.info("Sending AppendEntries (term ${localNodeState.currentTerm})")
            val clusterNodeIdentifiers = messageSender.getAllNodes().minus(localNodeState.nodeIdentifier)

            clusterNodeIdentifiers.map { dstNodeIdentifier ->
                val nextIndexPerNode: Long = localNodeState.externalNodeStates[dstNodeIdentifier]!!.nextIndex
                val appendEntriesRequest: RaftMessage.AppendEntries =
                    buildAppendEntries(fromIndex = nextIndexPerNode)
                val matchIndexIfSuccess = nextIndexPerNode + appendEntriesRequest.entries.size - 1
                val deferredAppendEntriesResult: Deferred<AppendEntriesResult> = async {
                    kotlin.runCatching {
                        val appendEntriesResponse = messageSender.sendOrThrow(dstNodeIdentifier, appendEntriesRequest)
                                as RaftMessage.AppendEntriesResponse
                        AppendEntriesResult(
                            dstNodeIdentifier,
                            matchIndexIfSuccess,
                            appendEntriesResponse.entriesAppended
                        )
                    }.getOrElse {
                        LOGGER.trace("Exception while sending AppendEntries to $dstNodeIdentifier", it)
                        AppendEntriesResult(dstNodeIdentifier, matchIndexIfSuccess, false)
                    }
                }
                deferredAppendEntriesResult
            }.map { deferredAppendEntriesResult ->
                val appendEntriesResult = deferredAppendEntriesResult.await()
                if (appendEntriesResult.isSuccess) {
                    localNodeState.externalNodeStates[appendEntriesResult.dstNodeIdentifier]!!.matchIndex =
                        appendEntriesResult.matchIndex
                    localNodeState.externalNodeStates[appendEntriesResult.dstNodeIdentifier]!!.nextIndex =
                        appendEntriesResult.matchIndex + 1
                } else {
                    localNodeState.externalNodeStates[appendEntriesResult.dstNodeIdentifier]!!.nextIndex =
                        maxOf(
                            0,
                            localNodeState.externalNodeStates[appendEntriesResult.dstNodeIdentifier]!!.nextIndex - 1
                        )
                }
            }
            Unit
        }

    private fun buildAppendEntries(fromIndex: Long): RaftMessage.AppendEntries {
        if (fromIndex < 0) {
            error("fromIndex cannot be negative")
        }

        if (localNodeState.currentNodeType != NodeType.LEADER) {
            LOGGER.warn("${localNodeState.nodeIdentifier} :: cannot send appendEntries because node is not leader. currentNodeType = ${localNodeState.currentNodeType}")
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
                leaderIdentifier = localNodeState.nodeIdentifier,
                prevLogIndex = prevLogIndex ?: -1,
                prevLogTerm = prevLogTerm ?: -1,
                lastCommitIndex = lastCommitIndex ?: -1,
                entries = entries
            )
        }

        val lastLogTerm = lastLogIndex?.let { logStore.getLogEntryByIndex(it)!!.term }

        return RaftMessage.AppendEntries(
            term = localNodeState.currentTerm,
            leaderIdentifier = localNodeState.nodeIdentifier,
            prevLogIndex = lastLogIndex ?: -1,
            prevLogTerm = lastLogTerm ?: -1,
            lastCommitIndex = lastCommitIndex ?: -1,
            entries = emptyList()
        )
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}