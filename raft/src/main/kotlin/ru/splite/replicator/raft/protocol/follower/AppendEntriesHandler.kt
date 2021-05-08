package ru.splite.replicator.raft.protocol.follower

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.protocol.leader.CommitEntries
import ru.splite.replicator.raft.state.NodeStateStore
import ru.splite.replicator.raft.state.NodeType

internal class AppendEntriesHandler(
    private val localNodeStateStore: NodeStateStore,
    private val logStore: ReplicatedLogStore,
    private val commitEntries: CommitEntries,
    private val stateMutex: Mutex
) {

    suspend fun handleAppendEntries(request: RaftMessage.AppendEntries): RaftMessage.AppendEntriesResponse {
        stateMutex.withLock {
            val localNodeState = localNodeStateStore.getState()
            //игнорируем запрос из терма меньше текущего
            if (localNodeState.currentTerm > request.term) {
                LOGGER.debug("request skipped $request because of currentTerm ${localNodeState.currentTerm} < requestTerm ${request.term}")
                return RaftMessage.AppendEntriesResponse(term = localNodeState.currentTerm, entriesAppended = false)
            }

            if (localNodeState.currentTerm != request.term) {
                LOGGER.debug("detected new LEADER ${request.leaderIdentifier}")
            }

            localNodeStateStore.setState(
                localNodeState.copy(
                    currentTerm = request.term,
                    lastVotedLeaderIdentifier = null,
                    leaderIdentifier = request.leaderIdentifier,
                    currentNodeType = NodeType.FOLLOWER
                )
            )

            if (request.prevLogIndex >= 0) {
                val prevLogEntry = logStore.getLogEntryByIndex(request.prevLogIndex)
                    ?: return RaftMessage.AppendEntriesResponse(
                        term = localNodeState.currentTerm,
                        entriesAppended = false,
                        conflictIndex = logStore.lastLogIndex()?.plus(1L) ?: 0L
                    )

                if (prevLogEntry.term != request.prevLogTerm) {

                    var firstTermIndex = request.prevLogIndex

                    while (firstTermIndex > 0L && logStore
                            .getLogEntryByIndex(firstTermIndex - 1L)
                            ?.term == prevLogEntry.term
                    ) {
                        firstTermIndex -= 1L
                    }

                    return RaftMessage.AppendEntriesResponse(
                        term = request.term,
                        entriesAppended = false,
                        conflictIndex = firstTermIndex
                    )
                }
            }

            request.entries.forEachIndexed { index, logEntry ->
                val logIndex: Long = request.prevLogIndex + index + 1
                val conflictLogEntry = logStore.getLogEntryByIndex(logIndex)
                if (conflictLogEntry == null) {
                    logStore.setLogEntry(logIndex, logEntry)
                } else if (conflictLogEntry.term != logEntry.term) {
                    logStore.prune(logIndex)
                    logStore.setLogEntry(logIndex, logEntry)
                }
            }

            val lastCommitIndex = logStore.lastCommitIndex() ?: -1
            if (request.lastCommitIndex >= 0 && request.lastCommitIndex > lastCommitIndex) {
                logStore.commit(request.lastCommitIndex)
                commitEntries.fireCommitEventIfNeeded()
            }

            return RaftMessage.AppendEntriesResponse(term = request.term, entriesAppended = true)
        }
    }


    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}