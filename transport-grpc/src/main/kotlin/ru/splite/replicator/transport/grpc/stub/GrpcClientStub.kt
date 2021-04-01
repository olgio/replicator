package ru.splite.replicator.transport.grpc.stub

import com.google.protobuf.ByteString
import io.grpc.ConnectivityState
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.message.proto.BinaryMessageRequest
import ru.splite.replicator.message.proto.BinaryRpcGrpcKt
import ru.splite.replicator.transport.grpc.GrpcAddress
import ru.splite.replicator.transport.grpc.ShutdownSupportable
import java.util.concurrent.TimeUnit

internal class GrpcClientStub(override val address: GrpcAddress) : ClientStub, ShutdownSupportable {

    private val stub = createStub()

    override val unavailabilityRank: Int
        get() {
            return when ((stub.channel as ManagedChannel).getState(false)) {
                ConnectivityState.CONNECTING -> 1
                ConnectivityState.READY -> 0
                ConnectivityState.TRANSIENT_FAILURE -> 3
                ConnectivityState.IDLE -> 2
                ConnectivityState.SHUTDOWN -> Int.MAX_VALUE
            }
        }

    override suspend fun send(from: NodeIdentifier, bytes: ByteArray): ByteArray {
        val request = BinaryMessageRequest.newBuilder()
            .setFrom(from.identifier)
            .setMessage(ByteString.copyFrom(bytes))
            .build()

        val start = System.currentTimeMillis()
        val response = stub.call(request).message.toByteArray()
        LOGGER.debug("Sent message to $address in ${System.currentTimeMillis() - start} ms")
        return response
    }

    override fun shutdown() {
        (stub.channel as ManagedChannel).shutdown()
    }

    override fun awaitTermination() {
        (stub.channel as ManagedChannel).awaitTermination(5, TimeUnit.SECONDS)
    }

    private fun createStub(): BinaryRpcGrpcKt.BinaryRpcCoroutineStub {
        LOGGER.info("Initializing grpc stub for address $address")
        val channel = ManagedChannelBuilder.forAddress(address.host, address.port)
            .usePlaintext()
            .build()
        try {
            val connectivityState = channel.getState(true)
            LOGGER.info("Initialized grpc stub for address $address in state $connectivityState")
        } catch (e: Exception) {
            LOGGER.error("Cannot acquire grpc stub connection", e)
        }
        return BinaryRpcGrpcKt.BinaryRpcCoroutineStub(channel)
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}