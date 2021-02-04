package ru.splite.replicator.paxos

import org.slf4j.LoggerFactory
import ru.splite.replicator.LogStoreAssert
import ru.splite.replicator.bus.ClusterTopology
import ru.splite.replicator.paxos.message.PaxosMessage
import ru.splite.replicator.paxos.message.PaxosMessageReceiver
import ru.splite.replicator.raft.message.RaftMessage
import java.util.concurrent.atomic.AtomicBoolean

fun assertThatLogs(vararg nodes: PaxosProtocol): LogStoreAssert {
    return LogStoreAssert.assertThatLogs(*nodes.map { it.replicatedLogStore }.toTypedArray())
}

fun PaxosProtocolController.asManaged(): ManagedPaxosProtocolNode = ManagedPaxosProtocolNode(this)

suspend fun ClusterTopology<ManagedPaxosProtocolNode>.isolateNodes(
    vararg nodes: ManagedPaxosProtocolNode,
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

class ManagedPaxosProtocolNode(private val receiver: PaxosProtocolController) : PaxosProtocol by receiver,
    PaxosMessageReceiver {

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

    override suspend fun handleVoteRequest(request: PaxosMessage.VoteRequest): PaxosMessage.VoteResponse {
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