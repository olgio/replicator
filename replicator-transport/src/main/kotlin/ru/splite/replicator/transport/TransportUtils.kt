package ru.splite.replicator.transport

suspend fun CoroutineChannelTransport.isolateNodes(
    vararg nodes: Receiver,
    action: suspend () -> Unit
) {
    val openNodes = nodes.map { it.address }
    this.nodes.forEach { receiver ->
        this.setNodeIsolated(receiver, !openNodes.contains(receiver))
    }
    action.invoke()
}