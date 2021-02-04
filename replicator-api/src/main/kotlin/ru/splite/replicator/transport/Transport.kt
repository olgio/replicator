package ru.splite.replicator.transport

import ru.splite.replicator.bus.NodeIdentifier

interface Transport {

    val nodes: Collection<NodeIdentifier>

    fun subscribe(address: NodeIdentifier, actor: Actor)

    suspend fun send(actor: Actor, dst: NodeIdentifier, payload: ByteArray): ByteArray
}