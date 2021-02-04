package ru.splite.replicator.transport

import kotlinx.serialization.decodeFromByteArray
import kotlinx.serialization.encodeToByteArray
import kotlinx.serialization.protobuf.ProtoBuf
import ru.splite.replicator.bus.NodeIdentifier

suspend inline fun <reified C, reified D> Actor.sendProto(dst: NodeIdentifier, payload: C): D {
    val request = ProtoBuf.encodeToByteArray(payload)
    return ProtoBuf.decodeFromByteArray(this.send(dst, request))
}

suspend fun CoroutineChannelTransport.isolateNodes(
    vararg nodes: Actor,
    action: suspend () -> Unit
) {
    val openNodes = nodes.map { it.address }
    this.nodes.forEach { receiver ->
        if (openNodes.contains(receiver)) {
            this.open(receiver)
        } else {
            this.isolate(receiver)
        }
    }
    action.invoke()
}