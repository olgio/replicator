package ru.splite.replicator.raft.state.follower

import org.slf4j.LoggerFactory
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.RaftLocalNodeState

class VoteRequestHandler(
    private val localNodeState: RaftLocalNodeState,
    private val logStore: ReplicatedLogStore
) {

    fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse {

        //текущий терм больше полученного -> получили устаревший запрос -> отклоняем
        if (localNodeState.currentTerm > request.term) {
            LOGGER.debug("VoteRequest rejected: currentTerm ${localNodeState.currentTerm} > requestTerm ${request.term}. request = $request")
            return RaftMessage.VoteResponse(term = localNodeState.currentTerm, voteGranted = false)
        }

        //проверяем что лог не отстает
        val candidateCanBeLeader = candidateCanBeLeader(request)

        //текущий терм меньше полученного -> голосуем за нового лидера
        if (localNodeState.currentTerm < request.term) {
            localNodeState.currentTerm = request.term
            localNodeState.currentNodeType = NodeType.FOLLOWER
            if (candidateCanBeLeader) {
                localNodeState.lastVotedLeaderIdentifier = request.candidateIdentifier
                LOGGER.debug("VoteRequest accepted: currentTerm < requestTerm. request = $request")
                return RaftMessage.VoteResponse(term = localNodeState.currentTerm, voteGranted = true)
            }
        }

        if (!candidateCanBeLeader) {
            LOGGER.debug("VoteRequest rejected: detected stale log on candidate. request = $request")
            return RaftMessage.VoteResponse(term = localNodeState.currentTerm, voteGranted = false)
        }

        //термы равны -> конфликт
        if (localNodeState.currentTerm == request.term) {
            if (localNodeState.lastVotedLeaderIdentifier == null && localNodeState.currentNodeType != NodeType.LEADER) {
                localNodeState.lastVotedLeaderIdentifier = request.candidateIdentifier
            }
            val voteGranted: Boolean = localNodeState.lastVotedLeaderIdentifier == request.candidateIdentifier
            if (voteGranted) {
                LOGGER.debug("VoteRequest accepted. request = $request")
            } else {
                LOGGER.debug("VoteRequest rejected: detected another candidate ${localNodeState.lastVotedLeaderIdentifier} in this term. request = $request")
            }
            return RaftMessage.VoteResponse(
                term = localNodeState.currentTerm,
                voteGranted = voteGranted
            )
        }

        return RaftMessage.VoteResponse(term = localNodeState.currentTerm, voteGranted = false)
    }

    private fun candidateCanBeLeader(request: RaftMessage.VoteRequest): Boolean {
        //TODO
        val lastLogIndex: Long = logStore.lastLogIndex() ?: -1
        val lastLogTerm: Long = if (lastLogIndex < 0) -1 else logStore.getLogEntryByIndex(lastLogIndex)!!.term

        val candidateLastLogIndex = request.lastLogIndex
        val candidateLastLogTerm = request.lastLogTerm

        LOGGER.debug(
            "Voting for {}. lastLogTerm = [current = {}, candidate = {}], lastLogIndex = [current = {}, candidate = {}]",
            request.candidateIdentifier,
            lastLogTerm,
            candidateLastLogTerm,
            lastLogIndex,
            candidateLastLogIndex
        )

        if (lastLogTerm < 0 || candidateLastLogTerm > lastLogTerm) {
            return true
        }
        return lastLogTerm == request.lastLogTerm && lastLogIndex <= candidateLastLogIndex
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}