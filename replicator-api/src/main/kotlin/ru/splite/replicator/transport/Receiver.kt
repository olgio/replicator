package ru.splite.replicator.transport

interface Receiver {

    val address: NodeIdentifier

    suspend fun receive(src: NodeIdentifier, payload: ByteArray): ByteArray
}