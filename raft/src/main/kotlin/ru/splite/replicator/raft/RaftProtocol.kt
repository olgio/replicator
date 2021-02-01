package ru.splite.replicator.raft

import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.log.ReplicatedLogStore

interface RaftProtocol<C> {

    val nodeIdentifier: NodeIdentifier

    val replicatedLogStore: ReplicatedLogStore<C>

    val isLeader: Boolean

    suspend fun sendVoteRequestsAsCandidate(): Boolean

    suspend fun commitLogEntriesIfLeader()

    suspend fun sendAppendEntriesIfLeader()

    fun applyCommand(command: C)
}