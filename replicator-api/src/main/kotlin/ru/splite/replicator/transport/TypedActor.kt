package ru.splite.replicator.transport

import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.KSerializer
import kotlinx.serialization.protobuf.ProtoBuf
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier

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
        val start = System.currentTimeMillis()

        val requestBytes: ByteArray = binaryFormat.encodeToByteArray(this.serializer, payload)
        val responseBytes: ByteArray = transport.send(this, dst, requestBytes)
        val response = binaryFormat.decodeFromByteArray(this.serializer, responseBytes)

        LOGGER.debug("Sent message to $dst in ${System.currentTimeMillis() - start} ms: $payload ==> $response")
        return response
    }

    abstract suspend fun receive(src: NodeIdentifier, payload: T): T

    override suspend fun receive(src: NodeIdentifier, payload: ByteArray): ByteArray {
        val start = System.currentTimeMillis()

        val request: T = binaryFormat.decodeFromByteArray(this.serializer, payload)
        val response: T = receive(src, request)
        val responseBytes: ByteArray = binaryFormat.encodeToByteArray(this.serializer, response)

        LOGGER.debug("Handled message from $src in ${System.currentTimeMillis() - start} ms: $request ==> $response")
        return responseBytes
    }

    companion object {
        private val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}