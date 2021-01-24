package ru.splite.replicator.raft.state.leader;

import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.ClusterTopology
import ru.splite.replicator.raft.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessageReceiver
import ru.splite.replicator.raft.state.LocalNodeState
import ru.splite.replicator.raft.state.NodeType

public class CommitEntries<C>(
    private val localNodeState: LocalNodeState,
    private val logStore: ReplicatedLogStore<C>
) {

    fun commitLogEntriesIfLeader(clusterTopology: ClusterTopology<RaftMessageReceiver<*>>, majority: Int) {
        if (localNodeState.currentNodeType != NodeType.LEADER) {
            LOGGER.warn("${localNodeState.nodeIdentifier} :: cannot commit because node is not leader. currentNodeType = ${localNodeState.currentNodeType}")
            return
        }

        val clusterNodeIdentifiers = clusterTopology.nodes.minus(localNodeState.nodeIdentifier)

        val lastLogIndex: Long = logStore.lastLogIndex() ?: return
        val firstUncommittedIndex: Long = logStore.lastCommitIndex()?.plus(1) ?: 0

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
            if (logEntry.term != localNodeState.currentTerm) {
                LOGGER.warn("${localNodeState.nodeIdentifier} :: uncommitted logEntry with index $uncommittedIndex skipped because its term ${logEntry.term} != currentTerm ${localNodeState.currentTerm}")
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
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}
