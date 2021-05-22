package ru.splite.replicator.raft.protocol.follower

import org.slf4j.LoggerFactory
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.state.NodeStateStore
import ru.splite.replicator.raft.state.NodeType

internal class VoteRequestHandler(
    private val localNodeStateStore: NodeStateStore,
    private val logStore: ReplicatedLogStore
) {

    suspend fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse {
        localNodeStateStore.getState().let { localNodeState ->
            //текущий терм больше полученного -> получили устаревший запрос -> отклоняем
            if (localNodeState.currentTerm > request.term) {
                LOGGER.debug("VoteRequest rejected: currentTerm ${localNodeState.currentTerm} > requestTerm ${request.term}. request = $request")
                return RaftMessage.VoteResponse(term = localNodeState.currentTerm, voteGranted = false)
            }

            //проверяем что лог не отстает
            val candidateCanBeLeader = candidateCanBeLeader(request)

            //текущий терм меньше полученного -> голосуем за нового лидера
            if (localNodeState.currentTerm < request.term) {
                localNodeStateStore.setState(
                    localNodeState.copy(
                        currentTerm = request.term,
                        currentNodeType = NodeType.FOLLOWER,
                        lastVotedLeaderIdentifier = if (candidateCanBeLeader) request.candidateIdentifier else null
                    )
                )
                return if (candidateCanBeLeader) {
                    LOGGER.debug("VoteRequest accepted: currentTerm < requestTerm. request = $request")
                    RaftMessage.VoteResponse(term = localNodeState.currentTerm, voteGranted = true)
                } else {
                    LOGGER.debug("VoteRequest rejected: detected stale log on candidate. request = $request")
                    RaftMessage.VoteResponse(term = localNodeState.currentTerm, voteGranted = false)
                }
            }

            //термы равны -> конфликт
            if (localNodeState.currentTerm == request.term) {

                val isValidCandidate =
                    localNodeState.lastVotedLeaderIdentifier == null && localNodeState.currentNodeType != NodeType.LEADER
                val leaderIdentifier =
                    if (isValidCandidate) request.candidateIdentifier else localNodeState.lastVotedLeaderIdentifier

                if (isValidCandidate) {
                    localNodeStateStore.setState(
                        localNodeState.copy(
                            lastVotedLeaderIdentifier = leaderIdentifier
                        )
                    )
                }
                val voteGranted: Boolean = leaderIdentifier == request.candidateIdentifier
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
    }

    private suspend fun candidateCanBeLeader(request: RaftMessage.VoteRequest): Boolean {
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