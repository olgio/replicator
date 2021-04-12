package ru.splite.replicator.transport

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.channels.actor
import java.util.concurrent.ConcurrentHashMap

class CoroutineChannelTransport(
    private val coroutineScope: CoroutineScope,
    private val calculateDelay: (NodeIdentifier, NodeIdentifier) -> Long = { _, _ -> 0L }
) : Transport {

    private class ChannelMessage(
        val src: NodeIdentifier,
        val payload: ByteArray,
        val response: CompletableDeferred<ByteArray> = CompletableDeferred()
    )

    private val actors: MutableMap<NodeIdentifier, SendChannel<ChannelMessage>> = ConcurrentHashMap()

    private val isolatedNodes: MutableSet<NodeIdentifier> = mutableSetOf()

    override val nodes: Collection<NodeIdentifier>
        get() = actors.keys

    fun isNodeIsolated(nodeIdentifier: NodeIdentifier): Boolean {
        return isolatedNodes.contains(nodeIdentifier)
    }

    fun setNodeIsolated(nodeIdentifier: NodeIdentifier, isolated: Boolean) {
        if (isolated) {
            isolatedNodes.add(nodeIdentifier)
        } else {
            isolatedNodes.remove(nodeIdentifier)
        }
    }

    override fun subscribe(address: NodeIdentifier, receiver: Receiver) {
        val coroutineName = CoroutineName("$address|transport")
        val channel = coroutineScope.actor<ChannelMessage>(coroutineName + SupervisorJob(), Int.MAX_VALUE) {
            for (message in channel) {
                launch {
                    val messageDelay = calculateDelay(message.src, address)
                    if (messageDelay > 0) {
                        delay(messageDelay)
                    }
                    try {
                        if (isolatedNodes.contains(message.src)) {
                            throw NodeUnavailableException("Cannot receive message from node ${message.src} because it is isolated")
                        } else if (isolatedNodes.contains(address)) {
                            throw NodeUnavailableException("Node $address cannot receive message because it is isolated")
                        }
                        val response = receiver.receive(message.src, message.payload)
                        message.response.complete(response)
                    } catch (e: Throwable) {
                        message.response.completeExceptionally(e)
                    }
                }
            }
        }
        actors[address] = channel
    }

    override suspend fun send(receiver: Receiver, dst: NodeIdentifier, payload: ByteArray): ByteArray {
        val dstChannel = actors[dst] ?: error("Address ${dst} is not registered")
        val message = ChannelMessage(receiver.address, payload)
        dstChannel.send(message)
        return message.response.await()
    }
}