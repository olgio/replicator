package ru.splite.replicator.coroutines

import kotlinx.coroutines.channels.SendChannel
import ru.splite.replicator.bus.NodeIdentifier

class NodeTopology<E> {

    private val identifierToChannel: MutableMap<NodeIdentifier, SendChannel<E>> = mutableMapOf()

    fun getNodes(): Collection<NodeIdentifier> = identifierToChannel.keys

    fun registerChannel(nodeIdentifier: NodeIdentifier, channel: SendChannel<E>) {
        identifierToChannel[nodeIdentifier] = channel
    }

    fun getChannel(nodeIdentifier: NodeIdentifier): SendChannel<E>? = identifierToChannel[nodeIdentifier]
}