package ru.splite.replicator.raft.state.leader

import ru.splite.replicator.raft.log.LogEntry
import ru.splite.replicator.raft.log.ReplicatedLogStore
import ru.splite.replicator.raft.state.LocalNodeState
import ru.splite.replicator.raft.state.NodeType

class CommandAppender<C>(
    private val localNodeState: LocalNodeState,
    private val logStore: ReplicatedLogStore<C>
) {

    fun addCommand(command: C): Long {
        if (localNodeState.currentNodeType != NodeType.LEADER) {
            error("Only leader can add command")
        }
        return logStore.appendLogEntry(LogEntry(term = localNodeState.currentTerm, command = command))
    }
}