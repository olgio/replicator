package ru.splite.replicator.raft.protocol.leader

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.slf4j.LoggerFactory
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.event.CommitEvent
import ru.splite.replicator.raft.event.IndexWithTerm
import ru.splite.replicator.raft.state.NodeStateStore
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.transport.NodeIdentifier

internal class CommitEntries(
    private val localNodeStateStore: NodeStateStore,
    private val logStore: ReplicatedLogStore,
    private val logEntryCommittableCondition: (LogEntry, Long) -> Boolean
) {

    private val commitEventMutableFlow: MutableStateFlow<CommitEvent> =
        MutableStateFlow(CommitEvent(logStore.lastCommitIndex()))

    val commitEventFlow: StateFlow<CommitEvent> = commitEventMutableFlow

    fun fireCommitEventIfNeeded() {
        val lastCommitIndex = logStore.lastCommitIndex() ?: return

        if (commitEventFlow.value.index != lastCommitIndex) {
            val commitEvent = CommitEvent(lastCommitIndex)
            val isEmitted = commitEventMutableFlow.tryEmit(commitEvent)
            LOGGER.debug("Fire event=$commitEvent, isEmitted=$isEmitted")
        }
    }

    suspend fun commitLogEntriesIfLeader(
        nodeIdentifiers: Collection<NodeIdentifier>,
        quorumSize: Int
    ): IndexWithTerm? = localNodeStateStore.getState().let { localNodeState ->
        if (localNodeState.currentNodeType != NodeType.LEADER) {
            LOGGER.warn("cannot commit because node is not leader. currentNodeType = ${localNodeState.currentNodeType}")
            return null
        }

        LOGGER.info("CommitEntries (term ${localNodeState.currentTerm})")

        val lastLogIndex: Long = logStore.lastLogIndex() ?: return null
        val firstUncommittedIndex: Long = logStore.lastCommitIndex()?.plus(1) ?: 0

        if (firstUncommittedIndex > lastLogIndex) {
            return null
        }

        LOGGER.debug("lastLogIndex = $lastLogIndex, firstUncommittedIndex = $firstUncommittedIndex")

        val lastCommittableIndex: Long? = generateSequence(lastLogIndex) {
            it - 1
        }.takeWhile { uncommittedIndex ->
            if (uncommittedIndex < firstUncommittedIndex) {
                return@takeWhile false
            }

            val matchedNodesCount = nodeIdentifiers.count {
                localNodeStateStore.getExternalNodeState(it).matchIndex >= uncommittedIndex
            } + 1

            if (matchedNodesCount < quorumSize) {
                return@takeWhile false
            }

            val logEntry = logStore.getLogEntryByIndex(uncommittedIndex)
            if (logEntry == null) {
                LOGGER.error("uncommitted logEntry with index $uncommittedIndex skipped because doesn't exists in store")
                return@takeWhile false
            }
            if (!logEntryCommittableCondition.invoke(logEntry, localNodeState.currentTerm)) {
                LOGGER.warn("uncommitted logEntry with index $uncommittedIndex skipped because committable condition is not met")
                return@takeWhile false
            }

            return@takeWhile true
        }.firstOrNull()

        LOGGER.debug("lastCommittableIndex = $lastCommittableIndex")

        if (lastCommittableIndex != null) {
            logStore.commit(lastCommittableIndex)
            fireCommitEventIfNeeded()
            return IndexWithTerm(
                index = lastCommittableIndex,
                term = logStore.getLogEntryByIndex(lastCommittableIndex)!!.term
            )
        }

        return null
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}
