package ru.splite.replicator.transport

import ru.splite.replicator.bus.NodeIdentifier

interface Receiver {

    val address: NodeIdentifier

    suspend fun receive(src: NodeIdentifier, payload: ByteArray): ByteArray
}