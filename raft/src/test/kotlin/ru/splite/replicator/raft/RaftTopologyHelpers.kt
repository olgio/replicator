package ru.splite.replicator.raft

import org.slf4j.LoggerFactory
import ru.splite.replicator.LogStoreAssert
import ru.splite.replicator.bus.ClusterTopology
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.message.RaftMessageReceiver
import java.util.concurrent.atomic.AtomicBoolean

fun assertThatLogs(vararg nodes: RaftProtocol): LogStoreAssert {
    return LogStoreAssert.assertThatLogs(*nodes.map { it.replicatedLogStore }.toTypedArray())
}

fun RaftProtocolController.asManaged(): ManagedRaftProtocolNode = ManagedRaftProtocolNode(this)

suspend fun ClusterTopology<ManagedRaftProtocolNode>.isolateNodes(
    vararg nodes: ManagedRaftProtocolNode,
    action: suspend () -> Unit
) {
    this.nodes.map {
        this[it]
    }.forEach { receiver ->
        if (nodes.contains(receiver)) {
            receiver.up()
        } else {
            receiver.down()
        }
    }
    action.invoke()
}

class ManagedRaftProtocolNode(private val receiver: RaftProtocolController) : RaftProtocol by receiver,
    RaftMessageReceiver {

    private val isAvailable = AtomicBoolean(true)

    fun down() {
        isAvailable.set(false)
    }

    fun up() {
        isAvailable.set(true)
    }

    override suspend fun handleAppendEntries(request: RaftMessage.AppendEntries): RaftMessage.AppendEntriesResponse {
        throwIfNotAvailable()
        return receiver.handleAppendEntries(request)
    }

    override suspend fun handleVoteRequest(request: RaftMessage.VoteRequest): RaftMessage.VoteResponse {
        throwIfNotAvailable()
        return receiver.handleVoteRequest(request)
    }

    private fun throwIfNotAvailable() {
        if (!isAvailable.get()) {
            error("Node is unavailable")
        }
    }

    companion object {
        val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}