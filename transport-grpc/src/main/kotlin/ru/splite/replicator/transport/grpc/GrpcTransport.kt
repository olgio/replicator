package ru.splite.replicator.transport.grpc

import com.google.common.base.Stopwatch
import com.google.protobuf.ByteString
import io.grpc.Server
import io.grpc.netty.NettyServerBuilder
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asExecutor
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.splite.replicator.message.proto.BinaryMessageRequest
import ru.splite.replicator.message.proto.BinaryMessageResponse
import ru.splite.replicator.message.proto.BinaryRpcGrpcKt
import ru.splite.replicator.metrics.Metrics
import ru.splite.replicator.metrics.Metrics.recordStopwatch
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.Receiver
import ru.splite.replicator.transport.Transport
import ru.splite.replicator.transport.grpc.stub.ClientStub
import ru.splite.replicator.transport.grpc.stub.GrpcClientStub
import java.io.Closeable

class GrpcTransport(addresses: Map<NodeIdentifier, GrpcAddress>) : Transport, Closeable {

    private var stubs: Map<NodeIdentifier, ClientStub> = addresses.map { (nodeIdentifier, address) ->
        nodeIdentifier to GrpcClientStub(address)
    }.toMap()

    override val nodes: Collection<NodeIdentifier>
        get() = stubs.entries.shuffled().sortedBy { it.value.unavailabilityRank }.map { it.key }

    override fun subscribe(address: NodeIdentifier, actor: Receiver) {
        val grpcAddress = stubs[address]?.address
            ?: error("Cannot find address for $address to subscribe")
        val serverStub = GrpcServer(grpcAddress, actor)
        stubs = stubs.toMutableMap().apply {
            put(address, serverStub)
        }
        serverStub.start()
        LOGGER.debug("Subscribed $address to listen $grpcAddress")
    }

    override suspend fun send(receiver: Receiver, dst: NodeIdentifier, payload: ByteArray): ByteArray {
        val stub = stubs[dst] ?: error("Cannot find address for $dst")
        return stub.send(receiver.address, payload)
    }

    suspend fun pingAll(): Int {
        val from = stubs.filter { it.value is GrpcServer }.keys.first()
        return stubs.values.map {
            kotlin.runCatching {
                it.ping(from)
            }
        }.count { it.isSuccess }
    }

    fun awaitTermination() {
        stubs.values.filterIsInstance<GrpcServer>().forEach {
            it.awaitTermination()
        }
    }

    override fun close() {
        stubs.values.filterIsInstance<ShutdownSupportable>().onEach {
            it.shutdown()
        }.forEach {
            it.awaitTermination()
        }
    }

    private inner class GrpcServer(
        override val address: GrpcAddress,
        private val receiver: Receiver
    ) : ClientStub, ShutdownSupportable {

        private val server: Server = NettyServerBuilder
            .forPort(address.port)
            .executor(GRPC_POOL)
            .addService(BinaryRpcService(receiver))
            .build()

        override val unavailabilityRank: Int
            get() = Int.MIN_VALUE

        override suspend fun send(from: NodeIdentifier, bytes: ByteArray): ByteArray {
            LOGGER.trace("Received local message for $address")
            return receiver.receive(from, bytes)
        }

        override suspend fun ping(from: NodeIdentifier) = Unit

        fun start() {
            server.start()
            LOGGER.info("Server started listening on $address")
            Runtime.getRuntime().addShutdownHook(
                Thread {
                    LOGGER.info("*** shutting down gRPC server since JVM is shutting down")
                    this@GrpcServer.shutdown()
                    this@GrpcServer.awaitTermination()
                    LOGGER.info("*** server shut down")
                }
            )
        }

        override fun shutdown() {
            server.shutdown()
        }

        override fun awaitTermination() {
            server.awaitTermination()
        }
    }

    private inner class BinaryRpcService(private val receiver: Receiver) :
        BinaryRpcGrpcKt.BinaryRpcCoroutineImplBase() {
        override suspend fun call(request: BinaryMessageRequest): BinaryMessageResponse {
            try {
                val src = NodeIdentifier(request.from)
                check(stubs.containsKey(src)) {
                    "Cannot receive message from $src because stub not found"
                }
                if (request.ping) {
                    return BinaryMessageResponse
                        .newBuilder()
                        .setMessage(request.message)
                        .build()
                }
                val stopwatch = Stopwatch.createStarted()
                val responseBytes = receiver.receive(src, request.message.toByteArray())
                Metrics.registry.receiveMessageLatency.recordStopwatch(stopwatch.stop())
                return BinaryMessageResponse
                    .newBuilder()
                    .setMessage(ByteString.copyFrom(responseBytes))
                    .build()
            } catch (e: Exception) {
                LOGGER.error("Error while handling message", e)
                throw e
            }
        }
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)

        val GRPC_POOL = Dispatchers.Default.asExecutor()
    }
}