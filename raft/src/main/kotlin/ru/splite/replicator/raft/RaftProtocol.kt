package ru.splite.replicator.raft

import kotlinx.coroutines.flow.StateFlow
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.event.AppendEntryEvent
import ru.splite.replicator.raft.event.CommitEvent
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.message.RaftMessageReceiver
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.sender.MessageSender
import java.time.Instant

interface RaftProtocol : RaftMessageReceiver {

    val address: NodeIdentifier

    val replicatedLogStore: ReplicatedLogStore

    val isLeader: Boolean

    val commitEventFlow: StateFlow<CommitEvent>

    val appendEntryEventFlow: StateFlow<AppendEntryEvent>

    val leaderAliveEventFlow: StateFlow<Instant>

    suspend fun sendVoteRequestsAsCandidate(messageSender: MessageSender<RaftMessage>): Boolean

    suspend fun commitLogEntriesIfLeader(messageSender: MessageSender<RaftMessage>)

    suspend fun sendAppendEntriesIfLeader(messageSender: MessageSender<RaftMessage>)

    suspend fun applyCommand(command: ByteArray): Long
}