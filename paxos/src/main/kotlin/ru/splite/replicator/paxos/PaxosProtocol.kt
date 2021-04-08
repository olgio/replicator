package ru.splite.replicator.paxos

import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.paxos.message.PaxosMessageReceiver
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.sender.MessageSender

interface PaxosProtocol : PaxosMessageReceiver {

    val address: NodeIdentifier

    val replicatedLogStore: ReplicatedLogStore

    val isLeader: Boolean

    suspend fun sendVoteRequestsAsCandidate(messageSender: MessageSender<RaftMessage>): Boolean

    suspend fun commitLogEntriesIfLeader(messageSender: MessageSender<RaftMessage>)

    suspend fun sendAppendEntriesIfLeader(messageSender: MessageSender<RaftMessage>)

    fun applyCommand(command: ByteArray): Long
}