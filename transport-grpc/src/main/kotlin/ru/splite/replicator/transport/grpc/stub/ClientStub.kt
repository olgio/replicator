package ru.splite.replicator.transport.grpc.stub

import ru.splite.replicator.transport.grpc.GrpcAddress

internal interface ClientStub {

    val address: GrpcAddress

    suspend fun send(bytes: ByteArray): ByteArray
}