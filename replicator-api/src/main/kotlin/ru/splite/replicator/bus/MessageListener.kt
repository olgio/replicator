package ru.splite.replicator.bus

interface MessageListener<T> {

    suspend fun handleMessage(from: NodeIdentifier, message: T)
}