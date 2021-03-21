package ru.splite.replicator.transport.grpc.stub

import com.google.protobuf.ByteString
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.splite.replicator.message.proto.BinaryMessageRequest
import ru.splite.replicator.message.proto.BinaryRpcGrpcKt
import ru.splite.replicator.transport.grpc.GrpcAddress
import ru.splite.replicator.transport.grpc.ShutdownSupportable
import java.util.concurrent.TimeUnit

internal class GrpcLazyClientStub(override val address: GrpcAddress) : ClientStub, ShutdownSupportable {

    private val stub by lazy {
        LOGGER.info("Initializing grpc stub for address $address")
        val channel = ManagedChannelBuilder.forAddress(address.host, address.port).usePlaintext().build()
        BinaryRpcGrpcKt.BinaryRpcCoroutineStub(channel)
    }

    override suspend fun send(bytes: ByteArray): ByteArray {
        val request = BinaryMessageRequest.newBuilder().setMessage(ByteString.copyFrom(bytes)).build()
        return stub.call(request).message.toByteArray()
    }

    override fun shutdown() {
        (stub.channel as ManagedChannel).shutdown()
    }

    override fun awaitTermination() {
        (stub.channel as ManagedChannel).awaitTermination(5, TimeUnit.SECONDS)
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}