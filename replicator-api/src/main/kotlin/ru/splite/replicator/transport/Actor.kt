package ru.splite.replicator.transport

import ru.splite.replicator.bus.NodeIdentifier

abstract class Actor(
    val address: NodeIdentifier,
    private val transport: Transport
) {

    init {
        transport.subscribe(address, this)
    }

    suspend fun send(dst: NodeIdentifier, payload: ByteArray): ByteArray {
        return transport.send(this, dst, payload)
    }

    abstract suspend fun receive(src: NodeIdentifier, payload: ByteArray): ByteArray
}