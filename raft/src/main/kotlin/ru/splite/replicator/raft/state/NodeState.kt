package ru.splite.replicator.raft.state

import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.raft.log.LogEntry
import ru.splite.replicator.raft.log.ReplicatedLogStore
import ru.splite.replicator.raft.message.RaftMessage

class NodeState<C>(val nodeIdentifier: NodeIdentifier, val logStore: ReplicatedLogStore<C>) {

    var currentNodeType: NodeType = NodeType.FOLLOWER
        private set

    var currentTerm: Long = 0L
        private set

    var leaderIdentifier: NodeIdentifier? = null
        private set

    private var lastVotedLeaderIdentifier: NodeIdentifier? = null

    fun becomeCandidate(newTerm: Long): RaftMessage.VoteRequest {
        LOGGER.debug("$nodeIdentifier :: state transition $currentNodeType (term $currentTerm) -> CANDIDATE (term $newTerm)")
        this.currentTerm = newTerm
        this.lastVotedLeaderIdentifier = this.nodeIdentifier
        this.currentNodeType = NodeType.CANDIDATE

        val lastLogIndex: Long? = logStore.lastLogIndex()
        val lastLogTerm: Long? = lastLogIndex?.let { logStore.getLogEntryByIndex(it)!!.term }

        return RaftMessage.VoteRequest(
            term = this.currentTerm, candidateIdentifier = this.nodeIdentifier,
            lastLogIndex = lastLogIndex, lastLogTerm = lastLogTerm
        )
    }

    fun becomeLeader() {
        LOGGER.debug("$nodeIdentifier :: state transition $currentNodeType -> LEADER (term $currentTerm)")
        this.lastVotedLeaderIdentifier = null
        this.leaderIdentifier = this.nodeIdentifier
        this.currentNodeType = NodeType.LEADER
    }

    fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse {

        //текущий терм больше полученного -> получили устаревший запрос -> отклоняем
        if (this.currentTerm > request.term) {
            LOGGER.debug("$nodeIdentifier :: VoteRequest rejected: currentTerm ${this.currentTerm} > requestTerm ${request.term}. request = $request")
            return RaftMessage.VoteResponse(term = this.currentTerm, voteGranted = false)
        }

        //проверяем что лог не отстает
        if (!candidateCanBeLeader(request)) {
            LOGGER.debug("$nodeIdentifier :: VoteRequest rejected: detected stale log on candidate. request = $request")
            return RaftMessage.VoteResponse(term = this.currentTerm, voteGranted = false)
        }

        //текущий терм меньше полученного -> голосуем за нового лидера
        if (this.currentTerm < request.term) {
            this.currentTerm = request.term
            this.lastVotedLeaderIdentifier = request.candidateIdentifier
            currentNodeType = NodeType.FOLLOWER
            LOGGER.debug("$nodeIdentifier :: VoteRequest accepted: currentTerm < requestTerm. request = $request")
            return RaftMessage.VoteResponse(term = this.currentTerm, voteGranted = true)
        }

        //термы равны -> конфликт
        if (this.currentTerm == request.term) {
            if (this.lastVotedLeaderIdentifier == null && currentNodeType != NodeType.LEADER) {
                this.lastVotedLeaderIdentifier = request.candidateIdentifier
            }
            val voteGranted: Boolean = lastVotedLeaderIdentifier == request.candidateIdentifier
            if (voteGranted) {
                LOGGER.debug("$nodeIdentifier :: VoteRequest accepted. request = $request")
            } else {
                LOGGER.debug("$nodeIdentifier :: VoteRequest rejected: detected another candidate ${lastVotedLeaderIdentifier} in this term. request = $request")
            }
            return RaftMessage.VoteResponse(
                term = this.currentTerm,
                voteGranted = voteGranted
            )
        }

        return RaftMessage.VoteResponse(term = this.currentTerm, voteGranted = false)
    }

    private fun candidateCanBeLeader(request: RaftMessage.VoteRequest): Boolean {
        //TODO
        val lastLogIndex: Long = logStore.lastLogIndex() ?: -1
        val lastLogTerm: Long = if (lastLogIndex < 0) -1 else logStore.getLogEntryByIndex(lastLogIndex)!!.term

        val candidateLastLogIndex = request.lastLogIndex ?: -1
        val candidateLastLogTerm = request.lastLogTerm ?: -1

        LOGGER.debug(
            "{} voting for {}. lastLogTerm = [current = {}, candidate = {}], lastLogIndex = [current = {}, candidate = {}]",
            nodeIdentifier,
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

    fun handleAppendEntries(request: RaftMessage.AppendEntries<C>): RaftMessage.AppendEntriesResponse {

        //игнорируем запрос из терма меньше текущего
        if (this.currentTerm > request.term) {
            LOGGER.debug("$nodeIdentifier :: request skipped $request because of currentTerm ${this.currentTerm} < requestTerm ${request.term}")
            return RaftMessage.AppendEntriesResponse(term = this.currentTerm, entriesAppended = false)
        }

        if (this.currentTerm != request.term) {
            LOGGER.debug("$nodeIdentifier :: detected new LEADER ${request.leaderIdentifier}")
        }
        this.currentTerm = request.term
        this.lastVotedLeaderIdentifier = null
        this.leaderIdentifier = request.leaderIdentifier
        this.currentNodeType = NodeType.FOLLOWER

        if (request.prevLogIndex != null) {
            val prevLogConsistent: Boolean = logStore.getLogEntryByIndex(request.prevLogIndex)?.let { prevLogEntry ->
                prevLogEntry.term == request.prevLogTerm
            } ?: false

            if (!prevLogConsistent) {
                return RaftMessage.AppendEntriesResponse(term = this.currentTerm, entriesAppended = false)
            }

        }

        request.entries.forEachIndexed { index, logEntry ->
            val logIndex: Long = (request.prevLogIndex ?: -1) + index + 1
            if (logStore.getLogEntryByIndex(logIndex) != null) {
                logStore.prune(logIndex)
            }
            logStore.setLogEntry(logIndex, logEntry)
        }


        if (request.lastCommitIndex != null) {
            logStore.commit(request.lastCommitIndex)
        }

        return RaftMessage.AppendEntriesResponse(term = this.currentTerm, entriesAppended = true)
    }

    fun buildAppendEntries(fromIndex: Long): RaftMessage.AppendEntries<C> {
        if (fromIndex < 0) {
            error("fromIndex cannot be negative")
        }

        val lastLogIndex: Long? = logStore.lastLogIndex()

        val lastCommitIndex: Long? = logStore.lastCommitIndex()

        if (lastLogIndex != null && fromIndex <= lastLogIndex) {

            val prevLogIndex: Long? = if (fromIndex > 0) fromIndex - 1 else null

            val prevLogTerm: Long? = prevLogIndex?.let { logStore.getLogEntryByIndex(it)!!.term }

            val entries = (fromIndex..lastLogIndex).map { logStore.getLogEntryByIndex(it)!! }.toList()

            return RaftMessage.AppendEntries(
                term = this.currentTerm,
                leaderIdentifier = this.nodeIdentifier,
                prevLogIndex = prevLogIndex,
                prevLogTerm = prevLogTerm,
                lastCommitIndex = lastCommitIndex,
                entries = entries
            )
        }

        val lastLogTerm = lastLogIndex?.let { logStore.getLogEntryByIndex(it)!!.term }

        return RaftMessage.AppendEntries(
            term = this.currentTerm,
            leaderIdentifier = this.nodeIdentifier,
            prevLogIndex = lastLogIndex,
            prevLogTerm = lastLogTerm,
            lastCommitIndex = lastCommitIndex,
            entries = emptyList()
        )
    }

    fun addCommand(command: C): Long {
        if (currentNodeType != NodeType.LEADER) {
            error("Only leader can add command")
        }
        return logStore.appendLogEntry(LogEntry(term = this.currentTerm, command = command))
    }

    companion object {
        val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}