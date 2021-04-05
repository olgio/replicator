package ru.splite.replicator.transport

interface Transport {

    val nodes: Collection<NodeIdentifier>

    fun subscribe(address: NodeIdentifier, actor: Receiver)

    suspend fun send(receiver: Receiver, dst: NodeIdentifier, payload: ByteArray): ByteArray
}