package ru.splite.replicator.raft

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.raft.log.InMemoryReplicatedLogStore
import ru.splite.replicator.raft.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.ClusterTopology
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.message.RaftMessageReceiver
import ru.splite.replicator.raft.state.NodeState
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.timer.RenewableTimerFactory
import java.util.*

class RaftProtocolController<C>(
    private val clusterTopology: ClusterTopology<C>,
    val nodeIdentifier: NodeIdentifier
) : RaftMessageReceiver<C> {

    val replicatedLogStore: ReplicatedLogStore<C> = InMemoryReplicatedLogStore()

    private val renewableTimerFactory = RenewableTimerFactory()

    private val nodeState = NodeState(nodeIdentifier, replicatedLogStore)

    //TODO refactor and integrate with Java Timer
//    private lateinit var termIncrementerTimerTask: RenewableTimerTask

    //TODO implement term timer stop/start and reinitializeIndex

    //TODO implement send entries and commit if majority
//    private lateinit var appendEntriesSenderTask: Timer

    private val clusterNodeIdentifiers: Collection<NodeIdentifier>
        get() = clusterTopology.nodes.minus(nodeIdentifier) //TODO

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

    suspend fun sendVoteRequestsAsCandidate() = coroutineScope {
        val nodes = clusterNodeIdentifiers

        if (nodes.isEmpty()) {
            error("Cluster cannot be empty")
        }
        val newTerm = nodeState.currentTerm + 1
        val voteRequest = nodeState.becomeCandidate(newTerm)
        val successResponses = nodes.map { dstNodeIdentifier ->
            async {
                clusterTopology[dstNodeIdentifier].handleVoteRequest(voteRequest)
            }
        }.mapNotNull {
            withTimeoutOrNull(2000) {
                it.await()
            }
        }

        val majority = Math.floorDiv(nodes.size, 2)
        if (successResponses.size >= majority) {
            nodeState.becomeLeader()
        }
    }

    suspend fun commitLogEntriesIfLeader() = coroutineScope {
        if (nodeState.currentNodeType != NodeType.LEADER) {
            LOGGER.warn("$nodeIdentifier :: cannot commit because node is not leader. currentNodeType = ${nodeState.currentNodeType}")
            return@coroutineScope
        }
        val firstUncommittedIndex: Long = replicatedLogStore.lastCommitIndex()?.plus(1) ?: 0
        LOGGER.debug("$nodeIdentifier :: firstUncommittedIndex = $firstUncommittedIndex")


        val lastCommittableIndex: Long? = generateSequence(firstUncommittedIndex) {
            it + 1
        }.takeWhile { uncommittedIndex ->
            //TODO check cluster
            val logEntry = replicatedLogStore.getLogEntryByIndex(uncommittedIndex)
            if (logEntry == null) {
                LOGGER.warn("$nodeIdentifier :: uncommitted logEntry with index $uncommittedIndex skipped because doesn't exists in store")
                return@takeWhile false
            }
            if (logEntry.term != nodeState.currentTerm) {
                LOGGER.warn("$nodeIdentifier :: uncommitted logEntry with index $uncommittedIndex skipped because its term ${logEntry.term} != currentTerm ${nodeState.currentTerm}")
                return@takeWhile false
            }
            return@takeWhile true
        }.lastOrNull()

        LOGGER.debug("$nodeIdentifier :: lastCommittableIndex = $lastCommittableIndex")

        if (lastCommittableIndex != null) {
            replicatedLogStore.commit(lastCommittableIndex)
        }
    }

    suspend fun sendAppendEntriesIfLeader() = coroutineScope {
        if (nodeState.currentNodeType != NodeType.LEADER) {
            LOGGER.warn("$nodeIdentifier :: cannot send appendEntries because node is not leader. currentNodeType = ${nodeState.currentNodeType}")
            return@coroutineScope
        }
        clusterNodeIdentifiers.map { dstNodeIdentifier ->
            val appendEntriesRequest = nodeState.buildAppendEntries(0) // TODO extract index
            async {
                clusterTopology[dstNodeIdentifier].handleAppendEntries(appendEntriesRequest)
            }
        }.mapNotNull {
            withTimeoutOrNull(1000) {
                it.await()
            }
        }
        //TODO handle results
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

    fun applyCommand(command: C) {
        nodeState.addCommand(command)
    }

    companion object {
        val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}