package ru.splite.replicator.paxos

import org.slf4j.LoggerFactory
import ru.splite.replicator.LogStoreAssert
import ru.splite.replicator.bus.ClusterTopology
import ru.splite.replicator.paxos.message.PaxosMessage
import ru.splite.replicator.paxos.message.PaxosMessageReceiver
import ru.splite.replicator.raft.message.RaftMessage
import java.util.concurrent.atomic.AtomicBoolean

fun <C> assertThatLogs(vararg nodes: PaxosProtocol<C>): LogStoreAssert<C> {
    return LogStoreAssert.assertThatLogs(*nodes.map { it.replicatedLogStore }.toTypedArray())
}

fun <C> PaxosProtocolController<C>.asManaged(): ManagedPaxosProtocolNode<C> = ManagedPaxosProtocolNode(this)

suspend fun <C> ClusterTopology<ManagedPaxosProtocolNode<C>>.isolateNodes(
    vararg nodes: ManagedPaxosProtocolNode<C>,
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

class ManagedPaxosProtocolNode<C>(private val receiver: PaxosProtocolController<C>) : PaxosProtocol<C> by receiver,
    PaxosMessageReceiver<C> {

    private val isAvailable = AtomicBoolean(true)

    fun down() {
        isAvailable.set(false)
    }

    fun up() {
        isAvailable.set(true)
    }

    override suspend fun handleAppendEntries(request: RaftMessage.AppendEntries<C>): RaftMessage.AppendEntriesResponse {
        throwIfNotAvailable()
        return receiver.handleAppendEntries(request)
    }

    override suspend fun handleVoteRequest(request: PaxosMessage.VoteRequest): PaxosMessage.VoteResponse<C> {
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