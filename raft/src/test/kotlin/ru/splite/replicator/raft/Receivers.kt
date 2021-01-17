package ru.splite.replicator.raft

import org.slf4j.LoggerFactory
import ru.splite.replicator.raft.message.ClusterTopology
import ru.splite.replicator.raft.message.RaftMessage
import ru.splite.replicator.raft.message.RaftMessageReceiver
import java.util.concurrent.atomic.AtomicBoolean

fun <C> RaftProtocolController<C>.asManaged(): ManagedRaftProtocolNode<C> = ManagedRaftProtocolNode(this)

suspend fun <C> ClusterTopology<ManagedRaftProtocolNode<C>>.isolateNodes(
    vararg nodes: ManagedRaftProtocolNode<C>,
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

class ManagedRaftProtocolNode<C>(private val receiver: RaftProtocolController<C>) : RaftProtocol<C> by receiver,
    RaftMessageReceiver<C> {

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