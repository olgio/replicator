package ru.splite.replicator.transport

import kotlinx.serialization.BinaryFormat
import kotlinx.serialization.KSerializer
import kotlinx.serialization.protobuf.ProtoBuf
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
        val requestBytes = binaryFormat.encodeToByteArray(this.serializer, payload)
        val responseBytes = transport.send(this, dst, requestBytes)
        return binaryFormat.decodeFromByteArray(this.serializer, responseBytes)
    }

    abstract suspend fun receive(src: NodeIdentifier, payload: T): T

    override suspend fun receive(src: NodeIdentifier, payload: ByteArray): ByteArray {
        val requestBytes = binaryFormat.decodeFromByteArray(this.serializer, payload)
        val responseBytes = receive(src, requestBytes)
        return binaryFormat.encodeToByteArray(this.serializer, responseBytes)
    }
}