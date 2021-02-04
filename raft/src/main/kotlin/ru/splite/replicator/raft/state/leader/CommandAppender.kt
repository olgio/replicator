package ru.splite.replicator.raft.state.leader

import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.RaftLocalNodeState

class CommandAppender(
    private val localNodeState: RaftLocalNodeState,
    private val logStore: ReplicatedLogStore
) {

    fun addCommand(command: ByteArray): Long {
        if (localNodeState.currentNodeType != NodeType.LEADER) {
            error("Only leader can add command")
        }
        return logStore.appendLogEntry(LogEntry(term = localNodeState.currentTerm, command = command))
    }
}