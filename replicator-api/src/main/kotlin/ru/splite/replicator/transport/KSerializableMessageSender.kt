package ru.splite.replicator.transport

import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.KSerializer
import kotlinx.serialization.protobuf.ProtoBuf
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier

class KSerializableMessageSender<T>(
    private val actor: Actor,
    private val defaultTimeout: Long = 1000L,
    private val kSerializer: KSerializer<T>,
    private val binaryFormat: BinaryFormat = ProtoBuf
) {

    private data class RoutedResponse<T>(val dst: NodeIdentifier, val response: T?)

//    suspend fun sendToAll(nodeIdentifiers: Collection<NodeIdentifier>,
//                          payload: T,
//                          timeout: Long? = null): Map<NodeIdentifier, T> = coroutineScope {
//        nodeIdentifiers.map { dst ->
//            val deferred = async {
//                sendOrNull(dst, payload, timeout)
//            }
//            dst to deferred
//        }.mapNotNull { (dst, deferred) ->
//            val response = deferred.await()
//            if (response != null) {
//                dst to response
//            } else {
//                null
//            }
//        }.toMap()
//    }

    fun getNearestNodes(n: Int): Set<NodeIdentifier> {
        return this.actor.transport.nodes.sortedByDescending { if (it == this.actor.address) 1 else 0 }.toSet()
    }

    fun getAllNodes(): Set<NodeIdentifier> {
        return getNearestNodes(this.actor.transport.nodes.size)
    }

    suspend fun sendToQuorum(
        nodeIdentifiers: Collection<NodeIdentifier>,
        quorumSize: Int = nodeIdentifiers.size,
        timeout: Long? = null,
        isSuccessFilter: (T) -> Boolean = { true },
        payloadAction: (NodeIdentifier) -> T
    ): Map<NodeIdentifier, T> = coroutineScope {
        val fullSize = nodeIdentifiers.size
        if (fullSize < quorumSize) {
            error("Required quorumSize $quorumSize but received only $fullSize node identifiers")
        }
        val remainingSize = fullSize - quorumSize
        val channel = Channel<RoutedResponse<T>>(capacity = fullSize)
        val jobs = nodeIdentifiers.map { dst ->
            launch {
                val response = sendOrNull(dst, payloadAction.invoke(dst), timeout)
                channel.send(RoutedResponse(dst, response))
            }
        }

        val responses = mutableMapOf<NodeIdentifier, T>()
        val failures = mutableSetOf<NodeIdentifier>()
        for (routedResponse in channel) {
            if (routedResponse.response != null) {
                responses[routedResponse.dst] = routedResponse.response
            } else {
                failures.add(routedResponse.dst)
            }
            val successResponsesSize = responses.count { isSuccessFilter(it.value) }
            val rejectedResponsesSize = responses.size - successResponsesSize + failures.size
            if (successResponsesSize >= quorumSize || rejectedResponsesSize > remainingSize) {
                break
            }
        }

        jobs.forEach {
            it.cancel("Quorum with size $quorumSize already completed")
        }

        responses
    }

    suspend fun sendOrNull(
        dst: NodeIdentifier, payload: T,
        timeout: Long? = null
    ): T? {
        val result = runCatching<T> {
            withTimeout(timeout ?: this.defaultTimeout) {

                val requestBytes = binaryFormat.encodeToByteArray(kSerializer, payload)
                val responseBytes = actor.send(dst, requestBytes)
                binaryFormat.decodeFromByteArray(kSerializer, responseBytes)
            }
        }
        if (result.isFailure) {
            LOGGER.error("Failed request ${actor.address} -> $dst: ${result.exceptionOrNull()}")
        }
        return result.getOrNull()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}