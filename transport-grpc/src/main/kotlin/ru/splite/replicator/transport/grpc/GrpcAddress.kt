package ru.splite.replicator.transport.grpc

data class GrpcAddress(val host: String, val port: Int) {

    override fun toString(): String {
        return "$host:$port"
    }
}
