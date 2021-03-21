package ru.splite.replicator.transport.grpc

interface ShutdownSupportable {

    fun shutdown()

    fun awaitTermination()
}