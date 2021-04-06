package ru.splite.replicator.raft

import kotlinx.coroutines.flow.StateFlow
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.state.leader.LastCommitEvent
import ru.splite.replicator.transport.NodeIdentifier
import java.time.Instant

interface RaftProtocol {

    val address: NodeIdentifier

    val replicatedLogStore: ReplicatedLogStore

    val isLeader: Boolean

    val lastCommitIndexFlow: StateFlow<LastCommitEvent>

    val leaderAliveFlow: StateFlow<Instant>

    suspend fun sendVoteRequestsAsCandidate(): Boolean

    suspend fun commitLogEntriesIfLeader()

    suspend fun sendAppendEntriesIfLeader()

    suspend fun applyCommand(command: ByteArray): Long
}