package ru.splite.replicator.coroutines

import kotlinx.coroutines.channels.ActorScope
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.MessageBus
import ru.splite.replicator.bus.NodeIdentifier

class NodeScope<E>(
    val nodeIdentifier: NodeIdentifier,
    private val nodeTopology: NodeTopology<E>,
    actorScope: ActorScope<E>
) : MessageBus<E>, ActorScope<E> by actorScope {

    override fun getNodes(): Collection<NodeIdentifier> = nodeTopology.getNodes()

    override suspend fun sendMessage(dstNodeIdentifier: NodeIdentifier, message: E) {
        val dstChannel = nodeTopology.getChannel(dstNodeIdentifier)
            ?: error("Cannot resolve channel by $dstNodeIdentifier")
        dstChannel.send(message)
        LOGGER.info("{} :: Sent message {} to {}", nodeIdentifier, message, dstNodeIdentifier)
    }

    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}