package ru.splite.replicator.paxos.protocol.follower

import org.slf4j.LoggerFactory
import ru.splite.replicator.log.LogEntry
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.NodeStateStore

internal class VoteRequestHandler(
    private val localNodeStateStore: NodeStateStore,
    private val logStore: ReplicatedLogStore
) {

    suspend fun handleVoteRequest(request: RaftMessage.PaxosVoteRequest): RaftMessage.PaxosVoteResponse {
        localNodeStateStore.getState().let { localNodeState ->
            //текущий терм больше полученного -> получили устаревший запрос -> отклоняем
            if (localNodeState.currentTerm > request.term) {
                LOGGER.debug("VoteRequest rejected: currentTerm ${localNodeState.currentTerm} > requestTerm ${request.term}. request = $request")
                return RaftMessage.PaxosVoteResponse(
                    term = localNodeState.currentTerm,
                    voteGranted = false,
                    entries = emptyList()
                )
            }

            val fromLogIndex: Long = request.leaderCommit + 1

            val entries = mutableListOf<LogEntry>()
            var currentIndex = fromLogIndex
            while (currentIndex >= 0) {
                val logEntry = logStore.getLogEntryByIndex(currentIndex) ?: break
                entries.add(logEntry)
                currentIndex++
            }

            return RaftMessage.PaxosVoteResponse(
                term = localNodeState.currentTerm,
                voteGranted = true,
                entries = entries
            )
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}