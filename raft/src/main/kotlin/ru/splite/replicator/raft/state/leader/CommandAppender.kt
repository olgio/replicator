package ru.splite.replicator.raft.state.leader

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.event.AppendEntryEvent
import ru.splite.replicator.raft.event.IndexWithTerm
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.RaftLocalNodeState

class CommandAppender(
    private val localNodeState: RaftLocalNodeState,
    private val logStore: ReplicatedLogStore
) {

    private val appendEntryEventMutableFlow: MutableStateFlow<AppendEntryEvent> =
        MutableStateFlow(AppendEntryEvent(logStore.lastLogIndex()))

    val appendEntryEventFlow: StateFlow<AppendEntryEvent> = appendEntryEventMutableFlow

    suspend fun addCommand(command: ByteArray): IndexWithTerm {
        val currentTerm = localNodeState.currentTerm

        if (localNodeState.currentNodeType != NodeType.LEADER) {
            error("Only leader can add command. currentTerm = ${currentTerm}, currentLeader = ${localNodeState.leaderIdentifier}")
        }
        val index = logStore.appendLogEntry(LogEntry(term = currentTerm, command = command))
        appendEntryEventMutableFlow.value = AppendEntryEvent(index)
        return IndexWithTerm(index = index, term = currentTerm)
    }
}