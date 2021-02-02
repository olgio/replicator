package ru.splite.replicator.paxos

import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.log.ReplicatedLogStore

interface PaxosProtocol<C> {

    val nodeIdentifier: NodeIdentifier

    val replicatedLogStore: ReplicatedLogStore<C>

    val isLeader: Boolean

    suspend fun sendVoteRequestsAsCandidate(): Boolean

    suspend fun commitLogEntriesIfLeader()

    suspend fun sendAppendEntriesIfLeader()

    fun applyCommand(command: C)
}