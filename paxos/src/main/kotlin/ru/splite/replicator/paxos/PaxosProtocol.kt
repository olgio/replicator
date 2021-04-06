package ru.splite.replicator.paxos

import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.transport.NodeIdentifier

interface PaxosProtocol {

    val address: NodeIdentifier

    val replicatedLogStore: ReplicatedLogStore

    val isLeader: Boolean

    suspend fun sendVoteRequestsAsCandidate(): Boolean

    suspend fun commitLogEntriesIfLeader()

    suspend fun sendAppendEntriesIfLeader()

    fun applyCommand(command: ByteArray)
}