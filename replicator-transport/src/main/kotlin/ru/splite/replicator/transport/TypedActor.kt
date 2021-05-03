package ru.splite.replicator.transport

import com.google.common.base.Stopwatch
import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.KSerializer
import kotlinx.serialization.protobuf.ProtoBuf
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

abstract class TypedActor<T>(
    override val address: NodeIdentifier,
    val transport: Transport,
    private val serializer: KSerializer<T>,
    private val binaryFormat: BinaryFormat = ProtoBuf
) : Receiver {

    init {
        transport.subscribe(address, this)
    }

    suspend fun send(dst: NodeIdentifier, payload: T): T {
        val transportWatch = Stopwatch.createUnstarted()
        val serializerWatch = Stopwatch.createUnstarted()

        serializerWatch.start()
        val requestBytes: ByteArray = binaryFormat.encodeToByteArray(this.serializer, payload)
        serializerWatch.stop()

        transportWatch.start()
        val responseBytes: ByteArray = transport.send(this, dst, requestBytes)
        transportWatch.stop()

        serializerWatch.start()
        val response = binaryFormat.decodeFromByteArray(this.serializer, responseBytes)
        serializerWatch.stop()

        LOGGER.debug(
            "Sent message to {} in {} ms, serialized in {} ms: {} ==> {}",
            dst, transportWatch.elapsed(TimeUnit.MILLISECONDS),
            serializerWatch.elapsed(TimeUnit.MILLISECONDS),
            payload, response
        )
        return response
    }

    abstract suspend fun receive(src: NodeIdentifier, payload: T): T

    override suspend fun receive(src: NodeIdentifier, payload: ByteArray): ByteArray {
        val start = System.currentTimeMillis()

        val request: T = binaryFormat.decodeFromByteArray(this.serializer, payload)
        val response: T = receive(src, request)
        val responseBytes: ByteArray = binaryFormat.encodeToByteArray(this.serializer, response)

        LOGGER.debug(
            "Handled message from {} in {} ms: {} ==> {}",
            src,
            System.currentTimeMillis() - start,
            request,
            response
        )
        return responseBytes
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}