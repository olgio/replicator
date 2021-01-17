package ru.splite.replicator.raft

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.raft.log.InMemoryReplicatedLogStore
import ru.splite.replicator.raft.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.ClusterTopology
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.message.RaftMessageReceiver
import ru.splite.replicator.raft.state.ExternalNodeState
import ru.splite.replicator.raft.state.NodeState
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.timer.RenewableTimerFactory
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class RaftProtocolController<C>(
    private val clusterTopology: ClusterTopology<RaftMessageReceiver<C>>,
    override val nodeIdentifier: NodeIdentifier
) : RaftMessageReceiver<C>, RaftProtocol<C> {

    override val replicatedLogStore: ReplicatedLogStore<C> = InMemoryReplicatedLogStore()

    private val renewableTimerFactory = RenewableTimerFactory()

    private val nodeState = NodeState(nodeIdentifier, replicatedLogStore)

    //TODO refactor and integrate with Java Timer
//    private lateinit var termIncrementerTimerTask: RenewableTimerTask

    //TODO implement term timer stop/start and reinitializeIndex

    //TODO implement send entries and commit if majority
//    private lateinit var appendEntriesSenderTask: Timer

    private val majority: Int
        get() = Math.floorDiv(clusterTopology.nodes.size, 2)

    private val clusterNodeIdentifiers: Collection<NodeIdentifier>
        get() = clusterTopology.nodes.minus(nodeIdentifier) //TODO

    private val externalNodeStates: MutableMap<NodeIdentifier, ExternalNodeState> = ConcurrentHashMap()

//    fun CoroutineScope.initializeTimers() {
//        termIncrementerTimerTask = renewableTimerFactory.scheduleWithPeriodAndInitialDelay(5000) {
//            launch {
//                if (nodeState.currentNodeType != NodeType.LEADER) {
//                    sendVoteRequestsAsCandidate()
//                }
//            }
//        }
//
//        appendEntriesSenderTask = fixedRateTimer(initialDelay = 3000, period = 3000) {
//            launch {
//                sendAppendEntriesIfLeader()
//                commitLogEntriesIfLeader()
//            }
//        }
//    }

    private fun reinitializeExternalNodeStates() {
        val lastLogIndex = nodeState.logStore.lastLogIndex()?.plus(1) ?: 0
        clusterNodeIdentifiers.forEach { dstNodeIdentifier ->
            externalNodeStates[dstNodeIdentifier] = ExternalNodeState(nextIndex = lastLogIndex, matchIndex = -1)
        }
    }

    override suspend fun sendVoteRequestsAsCandidate(): Boolean = coroutineScope {
        val nodes = clusterNodeIdentifiers

        if (nodes.isEmpty()) {
            error("Cluster cannot be empty")
        }
        val newTerm: Long = nodeState.currentTerm + 1
        val voteRequest: RaftMessage.VoteRequest = nodeState.becomeCandidate(newTerm)
        val successResponses = nodes.map { dstNodeIdentifier ->
            async {
                kotlin.runCatching {
                    val voteResponse: RaftMessage.VoteResponse = withTimeout(1000) {
                        clusterTopology[dstNodeIdentifier].handleVoteRequest(voteRequest)
                    }
                    voteResponse
                }.getOrNull()
            }
        }.mapNotNull {
            it.await()
        }.filter {
            it.voteGranted
        }

        LOGGER.info("$nodeIdentifier :: VoteResult for term ${nodeState.currentTerm}: ${successResponses.size + 1}/${nodes.size + 1} (majority = ${majority + 1})")
        if (successResponses.size >= majority) {
            nodeState.becomeLeader()
            reinitializeExternalNodeStates()
            true
        } else {
            false
        }
    }

    override suspend fun commitLogEntriesIfLeader() = coroutineScope {
        if (nodeState.currentNodeType != NodeType.LEADER) {
            LOGGER.warn("$nodeIdentifier :: cannot commit because node is not leader. currentNodeType = ${nodeState.currentNodeType}")
            return@coroutineScope
        }
        val lastLogIndex: Long = replicatedLogStore.lastLogIndex() ?: return@coroutineScope
        val firstUncommittedIndex: Long = replicatedLogStore.lastCommitIndex()?.plus(1) ?: 0

        LOGGER.debug("$nodeIdentifier :: lastLogIndex = $lastLogIndex, firstUncommittedIndex = $firstUncommittedIndex")

        val lastCommittableIndex: Long? = generateSequence(lastLogIndex) {
            it - 1
        }.takeWhile { uncommittedIndex ->
            if (uncommittedIndex < firstUncommittedIndex) {
                return@takeWhile false
            }
            val logEntry = replicatedLogStore.getLogEntryByIndex(uncommittedIndex)
            if (logEntry == null) {
                LOGGER.error("$nodeIdentifier :: uncommitted logEntry with index $uncommittedIndex skipped because doesn't exists in store")
                return@takeWhile false
            }
            if (logEntry.term != nodeState.currentTerm) {
                LOGGER.warn("$nodeIdentifier :: uncommitted logEntry with index $uncommittedIndex skipped because its term ${logEntry.term} != currentTerm ${nodeState.currentTerm}")
                return@takeWhile false
            }
            val countOfMatchedNodes = clusterNodeIdentifiers.count {
                externalNodeStates[it]!!.matchIndex >= uncommittedIndex
            }

            if (countOfMatchedNodes < majority) {
                return@takeWhile false
            }

            return@takeWhile true
        }.firstOrNull()

        LOGGER.debug("$nodeIdentifier :: lastCommittableIndex = $lastCommittableIndex")

        if (lastCommittableIndex != null) {
            replicatedLogStore.commit(lastCommittableIndex)
        }
    }

    override suspend fun sendAppendEntriesIfLeader() = coroutineScope {

        data class AppendEntriesResult(
            val dstNodeIdentifier: NodeIdentifier,
            val matchIndex: Long,
            val isSuccess: Boolean
        )

        if (nodeState.currentNodeType != NodeType.LEADER) {
            LOGGER.warn("$nodeIdentifier :: cannot send appendEntries because node is not leader. currentNodeType = ${nodeState.currentNodeType}")
            return@coroutineScope
        }
        clusterNodeIdentifiers.map { dstNodeIdentifier ->
            val nextIndexPerNode: Long = externalNodeStates[dstNodeIdentifier]!!.nextIndex
            val appendEntriesRequest: RaftMessage.AppendEntries<C> =
                nodeState.buildAppendEntries(fromIndex = nextIndexPerNode)
            val matchIndexIfSuccess = nextIndexPerNode + appendEntriesRequest.entries.size - 1
            val deferredAppendEntriesResult: Deferred<AppendEntriesResult> = async {
                kotlin.runCatching {
                    val appendEntriesResponse = withTimeout(1000) {
                        clusterTopology[dstNodeIdentifier].handleAppendEntries(appendEntriesRequest)
                    }
                    AppendEntriesResult(dstNodeIdentifier, matchIndexIfSuccess, appendEntriesResponse.entriesAppended)
                }.getOrElse {
                    AppendEntriesResult(dstNodeIdentifier, matchIndexIfSuccess, false)
                }
            }
            deferredAppendEntriesResult
        }.map { deferredAppendEntriesResult ->
            val appendEntriesResult = deferredAppendEntriesResult.await()
            if (appendEntriesResult.isSuccess) {
                externalNodeStates[appendEntriesResult.dstNodeIdentifier]!!.matchIndex = appendEntriesResult.matchIndex
                externalNodeStates[appendEntriesResult.dstNodeIdentifier]!!.nextIndex =
                    appendEntriesResult.matchIndex + 1
            } else {
                externalNodeStates[appendEntriesResult.dstNodeIdentifier]!!.nextIndex =
                    maxOf(0, externalNodeStates[appendEntriesResult.dstNodeIdentifier]!!.nextIndex - 1)
            }
        }
        Unit
    }

    override suspend fun handleAppendEntries(request: RaftMessage.AppendEntries<C>): RaftMessage.AppendEntriesResponse {
        LOGGER.debug("$nodeIdentifier :: received $request")
        val response = nodeState.handleAppendEntries(request)
        if (response.entriesAppended) {
            LOGGER.debug("$nodeIdentifier :: entries successfully appended $response")
//            termIncrementerTimerTask.renew()
        }
        return response
    }

    override suspend fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse {
        LOGGER.debug("$nodeIdentifier :: received $request")
        val response = nodeState.handleVoteRequest(request)
        if (response.voteGranted) {
            LOGGER.debug("$nodeIdentifier :: vote granted $response")
//            termIncrementerTimerTask.renew()
        }
        return response
    }

    override fun applyCommand(command: C) {
        nodeState.addCommand(command)
    }

    companion object {
        val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}