package ru.splite.replicator.transport.grpc.stub

import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.grpc.GrpcAddress

internal interface ClientStub {

    val address: GrpcAddress

    val unavailabilityRank: Int

    suspend fun send(from: NodeIdentifier, bytes: ByteArray): ByteArray
}