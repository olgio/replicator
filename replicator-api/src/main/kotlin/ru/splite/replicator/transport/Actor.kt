package ru.splite.replicator.transport

import ru.splite.replicator.bus.NodeIdentifier

abstract class Actor(
    override val address: NodeIdentifier,
    val transport: Transport
) : Receiver {

    init {
        transport.subscribe(address, this)
    }

    suspend fun send(dst: NodeIdentifier, payload: ByteArray): ByteArray {
        return transport.send(this, dst, payload)
    }

    abstract override suspend fun receive(src: NodeIdentifier, payload: ByteArray): ByteArray
}