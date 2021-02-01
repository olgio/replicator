package ru.splite.replicator.raft.state.follower

import org.slf4j.LoggerFactory
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.NodeType
import ru.splite.replicator.raft.state.RaftLocalNodeState

class VoteRequestHandler(
    private val localNodeState: RaftLocalNodeState,
    private val logStore: ReplicatedLogStore<*>
) {

    fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse {

        //текущий терм больше полученного -> получили устаревший запрос -> отклоняем
        if (localNodeState.currentTerm > request.term) {
            LOGGER.debug("${localNodeState.nodeIdentifier} :: VoteRequest rejected: currentTerm ${localNodeState.currentTerm} > requestTerm ${request.term}. request = $request")
            return RaftMessage.VoteResponse(term = localNodeState.currentTerm, voteGranted = false)
        }

        //проверяем что лог не отстает
        if (!candidateCanBeLeader(request)) {
            LOGGER.debug("${localNodeState.nodeIdentifier} :: VoteRequest rejected: detected stale log on candidate. request = $request")
            return RaftMessage.VoteResponse(term = localNodeState.currentTerm, voteGranted = false)
        }

        //текущий терм меньше полученного -> голосуем за нового лидера
        if (localNodeState.currentTerm < request.term) {
            localNodeState.currentTerm = request.term
            localNodeState.lastVotedLeaderIdentifier = request.candidateIdentifier
            localNodeState.currentNodeType = NodeType.FOLLOWER
            LOGGER.debug("${localNodeState.nodeIdentifier} :: VoteRequest accepted: currentTerm < requestTerm. request = $request")
            return RaftMessage.VoteResponse(term = localNodeState.currentTerm, voteGranted = true)
        }

        //термы равны -> конфликт
        if (localNodeState.currentTerm == request.term) {
            if (localNodeState.lastVotedLeaderIdentifier == null && localNodeState.currentNodeType != NodeType.LEADER) {
                localNodeState.lastVotedLeaderIdentifier = request.candidateIdentifier
            }
            val voteGranted: Boolean = localNodeState.lastVotedLeaderIdentifier == request.candidateIdentifier
            if (voteGranted) {
                LOGGER.debug("${localNodeState.nodeIdentifier} :: VoteRequest accepted. request = $request")
            } else {
                LOGGER.debug("${localNodeState.nodeIdentifier} :: VoteRequest rejected: detected another candidate ${localNodeState.lastVotedLeaderIdentifier} in this term. request = $request")
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

        val candidateLastLogIndex = request.lastLogIndex ?: -1
        val candidateLastLogTerm = request.lastLogTerm ?: -1

        LOGGER.debug(
            "{} voting for {}. lastLogTerm = [current = {}, candidate = {}], lastLogIndex = [current = {}, candidate = {}]",
            localNodeState.nodeIdentifier,
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