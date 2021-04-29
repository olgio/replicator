package ru.splite.replicator.raft.protocol.leader

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.event.AppendEntryEvent
import ru.splite.replicator.raft.event.IndexWithTerm
import ru.splite.replicator.raft.state.NodeStateStore
import ru.splite.replicator.raft.state.NodeType

internal class CommandAppender(
    private val localNodeStateStore: NodeStateStore,
    private val logStore: ReplicatedLogStore
) {

    private val appendEntryEventMutableFlow: MutableStateFlow<AppendEntryEvent> =
        MutableStateFlow(AppendEntryEvent(logStore.lastLogIndex()))

    val appendEntryEventFlow: StateFlow<AppendEntryEvent> = appendEntryEventMutableFlow

    suspend fun addCommand(command: ByteArray): IndexWithTerm {
        localNodeStateStore.getState().let { localNodeState ->
            val currentTerm = localNodeState.currentTerm

            if (localNodeState.currentNodeType != NodeType.LEADER) {
                error("Only leader can add command. currentState=${localNodeState.currentNodeType}")
            }
            val index = logStore.appendLogEntry(LogEntry(term = currentTerm, command = command))
            appendEntryEventMutableFlow.value = AppendEntryEvent(index)
            return IndexWithTerm(index = index, term = currentTerm)
        }
    }
}