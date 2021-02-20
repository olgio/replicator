package ru.splite.replicator.transport.sender

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.transport.TypedActor

class MessageSender<T>(
    private val actor: TypedActor<T>,
    private val defaultTimeout: Long = 1000L
) {

    fun getNearestNodes(n: Int): Set<NodeIdentifier> {
        return this.actor.transport.nodes.sortedByDescending { if (it == this.actor.address) 1 else 0 }.take(n).toSet()
    }

    fun getAllNodes(): Set<NodeIdentifier> {
        return getNearestNodes(this.actor.transport.nodes.size)
    }

//    suspend fun sendToQuorum(
//        nodeIdentifiers: Collection<NodeIdentifier>,
//        quorumSize: Int = nodeIdentifiers.size,
//        timeout: Long? = null,
//        payloadAction: (NodeIdentifier) -> T
//    ): Flow<RoutedResponse<T>> = coroutineScope {
//        val fullSize = nodeIdentifiers.size
//        if (fullSize < quorumSize) {
//            error("Required quorumSize $quorumSize but received only $fullSize node identifiers")
//        }
//        val remainingSize = fullSize - quorumSize
//
//        val channel = Channel<RoutedResponse<T?>>(capacity = fullSize)
//        val jobs = nodeIdentifiers.map { dst ->
//            launch {
//                val response = sendOrNull(dst, payloadAction.invoke(dst), timeout)
//                channel.send(RoutedResponse(dst, response))
//            }
//        }
//
//        flow<RoutedResponse<T>> {
//
//            var successSize = 0
//            var failuresSize = 0
//
//            for (routedResponse in channel) {
//                if (routedResponse.response != null) {
//                    successSize++
//                    emit(RoutedResponse(routedResponse.dst, routedResponse.response))
//                } else {
//                    failuresSize++
//                }
//                if (successSize >= quorumSize || failuresSize > remainingSize) {
//                    break
//                }
//            }
//            jobs.forEach {
//                it.cancel("Quorum with size $quorumSize already completed")
//            }
//        }
//    }

    suspend fun sendOrNull(dst: NodeIdentifier, payload: T, timeout: Long? = null): T? {
        val result = runCatching<T> {
            withTimeout(timeout ?: this.defaultTimeout) {
                actor.send(dst, payload)
            }
        }
        if (result.isFailure) {
            LOGGER.error("Failed request ${actor.address} -> $dst: ${result.exceptionOrNull()}")
        }
        return result.getOrNull()
    }

    suspend fun sendOrThrow(dst: NodeIdentifier, payload: T, timeout: Long? = null): T {
        return withTimeout(timeout ?: this.defaultTimeout) {
            actor.send(dst, payload)
        }
    }

    fun sendToQuorum(
        nodeIdentifiers: Collection<NodeIdentifier>,
        quorumSize: Int = nodeIdentifiers.size,
        timeout: Long? = null,
        payloadAction: (NodeIdentifier) -> T
    ): Flow<RoutedResponse<T>> {
        val fullSize = nodeIdentifiers.size
        if (fullSize < quorumSize) {
            error("Required quorumSize $quorumSize but received only $fullSize node identifiers")
        }
        val remainingSize = fullSize - quorumSize

        return flow<RoutedResponse<T>> {
            coroutineScope {
                val channel = Channel<RoutedResponse<T?>>(capacity = fullSize)
                val jobs = nodeIdentifiers.map { dst ->
                    launch {
                        val response = sendOrNull(dst, payloadAction.invoke(dst), timeout)
                        channel.send(RoutedResponse(dst, response))
                    }
                }

                var successSize = 0
                var failuresSize = 0
                for (routedResponse in channel) {
                    if (routedResponse.response != null) {
                        successSize++
                        emit(RoutedResponse(routedResponse.dst, routedResponse.response))
                    } else {
                        failuresSize++
                    }
                    if (successSize >= quorumSize || failuresSize > remainingSize) {
                        break
                    }
                }
                jobs.forEach {
                    it.cancel("Quorum with size $quorumSize already completed")
                }
            }
        }
    }

    suspend fun sendToAllOrThrow(
        nodeIdentifiers: Collection<NodeIdentifier>,
        timeout: Long? = null,
        payloadAction: (NodeIdentifier) -> T
    ): List<RoutedResponse<T>> = coroutineScope {
        nodeIdentifiers.map { dst ->
            async {
                val response = sendOrThrow(dst, payloadAction.invoke(dst), timeout)
                RoutedResponse(dst, response)
            }
        }.map {
            it.await()
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}