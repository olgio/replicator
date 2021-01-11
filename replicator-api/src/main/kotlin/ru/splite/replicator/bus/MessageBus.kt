package ru.splite.replicator.bus

interface MessageBus<T> {

    fun getNodes(): Collection<NodeIdentifier>

    suspend fun sendMessage(dstNodeIdentifier: NodeIdentifier, message: T)
}