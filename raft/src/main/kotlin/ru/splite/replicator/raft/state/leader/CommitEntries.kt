package ru.splite.replicator.raft.state.leader

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import org.slf4j.LoggerFactory
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.RaftLocalNodeState
import ru.splite.replicator.transport.Transport

class CommitEntries(
    private val localNodeState: RaftLocalNodeState,
    private val logStore: ReplicatedLogStore,
    private val logEntryCommittableCondition: (LogEntry, Long) -> Boolean
) {

    private val lastCommitIndexMutableFlow: MutableStateFlow<LastCommitEvent> =
        MutableStateFlow(LastCommitEvent(logStore.lastCommitIndex()))

    val lastCommitIndexFlow: StateFlow<LastCommitEvent> = lastCommitIndexMutableFlow

    fun commitLogEntriesIfLeader(transport: Transport, majority: Int) {
        if (localNodeState.currentNodeType != NodeType.LEADER) {
            LOGGER.warn("${localNodeState.nodeIdentifier} :: cannot commit because node is not leader. currentNodeType = ${localNodeState.currentNodeType}")
            return
        }

        LOGGER.info("CommitEntries (term ${localNodeState.currentTerm})")

        val clusterNodeIdentifiers = transport.nodes.minus(localNodeState.nodeIdentifier)

        val lastLogIndex: Long = logStore.lastLogIndex() ?: return
        val firstUncommittedIndex: Long = logStore.lastCommitIndex()?.plus(1) ?: 0

        if (firstUncommittedIndex > lastLogIndex) {
            return
        }

        LOGGER.debug("${localNodeState.nodeIdentifier} :: lastLogIndex = $lastLogIndex, firstUncommittedIndex = $firstUncommittedIndex")

        val lastCommittableIndex: Long? = generateSequence(lastLogIndex) {
            it - 1
        }.takeWhile { uncommittedIndex ->
            if (uncommittedIndex < firstUncommittedIndex) {
                return@takeWhile false
            }
            val logEntry = logStore.getLogEntryByIndex(uncommittedIndex)
            if (logEntry == null) {
                LOGGER.error("${localNodeState.nodeIdentifier} :: uncommitted logEntry with index $uncommittedIndex skipped because doesn't exists in store")
                return@takeWhile false
            }
            if (!logEntryCommittableCondition.invoke(logEntry, localNodeState.currentTerm)) {
                LOGGER.warn("${localNodeState.nodeIdentifier} :: uncommitted logEntry with index $uncommittedIndex skipped because committable condition is not met")
                return@takeWhile false
            }
            val matchedNodesCount = clusterNodeIdentifiers.count {
                localNodeState.externalNodeStates[it]!!.matchIndex >= uncommittedIndex
            } + 1

            if (matchedNodesCount < majority) {
                return@takeWhile false
            }

            return@takeWhile true
        }.firstOrNull()

        LOGGER.debug("${localNodeState.nodeIdentifier} :: lastCommittableIndex = $lastCommittableIndex")

        if (lastCommittableIndex != null) {
            logStore.commit(lastCommittableIndex)
            lastCommitIndexMutableFlow.tryEmit(LastCommitEvent(lastCommittableIndex))
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}
