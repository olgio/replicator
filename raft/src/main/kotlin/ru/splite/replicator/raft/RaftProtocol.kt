package ru.splite.replicator.raft

import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.raft.log.ReplicatedLogStore

interface RaftProtocol<C> {

    val nodeIdentifier: NodeIdentifier

    val replicatedLogStore: ReplicatedLogStore<C>

    suspend fun sendVoteRequestsAsCandidate(): Boolean

    suspend fun commitLogEntriesIfLeader()

    suspend fun sendAppendEntriesIfLeader()

    fun applyCommand(command: C)
}