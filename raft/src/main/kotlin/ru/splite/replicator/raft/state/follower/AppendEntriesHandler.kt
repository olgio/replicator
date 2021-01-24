package ru.splite.replicator.raft.state.follower

import org.slf4j.LoggerFactory
import ru.splite.replicator.raft.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.LocalNodeState
import ru.splite.replicator.raft.state.NodeType

class AppendEntriesHandler<C>(
    private val localNodeState: LocalNodeState,
    private val logStore: ReplicatedLogStore<C>
) {

    fun handleAppendEntries(request: RaftMessage.AppendEntries<C>): RaftMessage.AppendEntriesResponse {

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

        if (request.prevLogIndex != null) {
            val prevLogConsistent: Boolean = logStore.getLogEntryByIndex(request.prevLogIndex)?.let { prevLogEntry ->
                prevLogEntry.term == request.prevLogTerm
            } ?: false

            if (!prevLogConsistent) {
                return RaftMessage.AppendEntriesResponse(term = localNodeState.currentTerm, entriesAppended = false)
            }

        }

        request.entries.forEachIndexed { index, logEntry ->
            val logIndex: Long = (request.prevLogIndex ?: -1) + index + 1
            if (logStore.getLogEntryByIndex(logIndex) != null) {
                logStore.prune(logIndex)
            }
            logStore.setLogEntry(logIndex, logEntry)
        }


        if (request.lastCommitIndex != null) {
            logStore.commit(request.lastCommitIndex)
        }

        return RaftMessage.AppendEntriesResponse(term = localNodeState.currentTerm, entriesAppended = true)
    }


    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}