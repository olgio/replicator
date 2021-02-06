package ru.splite.replicator.raft.state.follower

import org.slf4j.LoggerFactory
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.RaftLocalNodeState

class AppendEntriesHandler(
    private val localNodeState: RaftLocalNodeState,
    private val logStore: ReplicatedLogStore
) {

    fun handleAppendEntries(request: RaftMessage.AppendEntries): RaftMessage.AppendEntriesResponse {

        //игнорируем запрос из терма меньше текущего
        if (localNodeState.currentTerm > request.term) {
            LOGGER.debug("${localNodeState.nodeIdentifier} :: request skipped $request because of currentTerm ${localNodeState.currentTerm} < requestTerm ${request.term}")
            return RaftMessage.AppendEntriesResponse(term = localNodeState.currentTerm, entriesAppended = false)
        }

        if (localNodeState.currentTerm != request.term) {
            LOGGER.debug("${localNodeState.nodeIdentifier} :: detected new LEADER ${request.leaderIdentifier}")
        }
        localNodeState.currentTerm = request.term
        localNodeState.lastVotedLeaderIdentifier = null
        localNodeState.leaderIdentifier = request.leaderIdentifier
        localNodeState.currentNodeType = NodeType.FOLLOWER

        if (request.prevLogIndex >= 0) {
            val prevLogConsistent: Boolean = logStore.getLogEntryByIndex(request.prevLogIndex)?.let { prevLogEntry ->
                prevLogEntry.term == request.prevLogTerm
            } ?: false

            if (!prevLogConsistent) {
                return RaftMessage.AppendEntriesResponse(term = localNodeState.currentTerm, entriesAppended = false)
            }

        }

        request.entries.forEachIndexed { index, logEntry ->
            val logIndex: Long = (request.prevLogIndex) + index + 1
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
        }

        return RaftMessage.AppendEntriesResponse(term = localNodeState.currentTerm, entriesAppended = true)
    }


    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}