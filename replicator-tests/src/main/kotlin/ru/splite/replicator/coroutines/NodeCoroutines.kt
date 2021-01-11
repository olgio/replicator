package ru.splite.replicator.coroutines

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.runBlocking
import ru.splite.replicator.bus.MessageListener
import ru.splite.replicator.bus.NodeIdentifier

fun <E> kotlinx.coroutines.CoroutineScope.topology(block: suspend TopologyScope<E>.() -> Unit) {
    val nodeTopology = NodeTopology<E>()
    val topologyScope = TopologyScope(nodeTopology, this)
    runBlocking {
        block.invoke(topologyScope)
    }
}

fun <E> TopologyScope<E>.node(name: String, block: suspend NodeScope<E>.() -> Unit): SendChannel<E> {
    val nodeIdentifier = NodeIdentifier(name)
    val channel = actor<E>(CoroutineName(nodeIdentifier.identifier), capacity = 10) {
        val nodeScope = NodeScope(nodeIdentifier, this@node.nodeTopology, this)
        block.invoke(nodeScope)
    }
    nodeTopology.registerChannel(nodeIdentifier, channel)
    return channel
}

fun <E> TopologyScope<E>.node(name: String, listener: MessageListener<E>) = node(name) {
    for (message in channel) {
        listener.handleMessage(this.nodeIdentifier, message)
    }
}