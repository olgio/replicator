package ru.splite.replicator.coroutines

import kotlinx.coroutines.CoroutineScope
import ru.splite.replicator.bus.NodeIdentifier

class TopologyScope<E>(internal val nodeTopology: NodeTopology<E>, coroutineScope: CoroutineScope) :
    CoroutineScope by coroutineScope {

    suspend fun sendMessage(dstNodeIdentifier: NodeIdentifier, message: E) {
        val dstChannel = nodeTopology.getChannel(dstNodeIdentifier)
            ?: error("Cannot resolve channel by $dstNodeIdentifier")
        dstChannel.send(message)
    }
}