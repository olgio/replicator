package ru.splite.replicator.raft.protocol

import kotlinx.coroutines.flow.StateFlow
import ru.splite.replicator.log.ReplicatedLogStore
import ru.splite.replicator.raft.event.AppendEntryEvent
import ru.splite.replicator.raft.event.CommitEvent
import ru.splite.replicator.raft.event.IndexWithTerm
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

    suspend fun commitLogEntriesIfLeader(messageSender: MessageSender<RaftMessage>): IndexWithTerm?

    suspend fun sendAppendEntriesIfLeader(messageSender: MessageSender<RaftMessage>)

    suspend fun appendCommand(command: ByteArray): IndexWithTerm

    suspend fun redirectAndAppendCommand(messageSender: MessageSender<RaftMessage>, command: ByteArray): IndexWithTerm

}