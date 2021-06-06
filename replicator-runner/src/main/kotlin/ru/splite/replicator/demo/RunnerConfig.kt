package ru.splite.replicator.demo

import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.default
import kotlinx.cli.required
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.grpc.GrpcAddress
import java.net.InetAddress

class RunnerConfig(parser: ArgParser) {

    enum class Protocol { RAFT, ATLAS, PAXOS }

    val threads by parser
        .option(type = ArgType.Int, description = "Count of threads")
        .default(10)

    val maxConcurrentSubmits by parser
        .option(type = ArgType.Int, description = "Max count of concurrent submits")
        .default(500)

    val f by parser
        .option(type = ArgType.Int, description = "Count of possible failures")
        .default(1)

    val messageTimeout by parser
        .option(type = ArgType.Int, description = "Message timeout ms")
        .default(1000 * 15)

    val commandExecutorTimeout by parser
        .option(type = ArgType.Int, description = "Command execution timeout ms")
        .default(1000 * 15)

    val port by parser
        .option(type = ArgType.Int, description = "KeyValue store port")
        .default(8000)

    val googleProjectId by parser
        .option(type = ArgType.String, description = "Google Project Id")

    val rocksDbFile by parser
        .option(type = ArgType.String, description = "RocksDb file ")

    private val protocolString by parser
        .option(fullName = "protocol", type = ArgType.String, description = "Protocol")
        .default(Protocol.ATLAS.name)

    private val processId by parser.option(type = ArgType.Int, description = "Process id")

    private val nodesString by parser
        .option(fullName = "nodes", type = ArgType.String, description = "List of nodes")
        .required()

    private val termClockPeriod by parser
        .option(type = ArgType.String, description = "Term clock timeout range, ms")
        .default("4000..8000")

    private val appendEntriesSendPeriod by parser
        .option(type = ArgType.String, description = "Append entries timeout range, ms")
        .default("1000..1000")

    val protocol by lazy {
        Protocol.valueOf(protocolString.toUpperCase())
    }

    val nodeIdentifier by lazy {
        val resultProcessId = processId ?: InetAddress.getLocalHost().hostName.extractStatefulSetOrdinaryId()
        NodeIdentifier(resultProcessId.toString())
    }

    val nodes by lazy {
        nodesString.parseAddressMap()
    }

    val termClockPeriodRange by lazy {
        termClockPeriod.asLongRange()
    }

    val appendEntriesSendPeriodRange by lazy {
        appendEntriesSendPeriod.asLongRange()
    }

    private fun String.parseAddressMap(): Map<NodeIdentifier, GrpcAddress> =
        this.split(",").map {
            val (addressHost, addressPort) = it.split(":", limit = 2)
            GrpcAddress(addressHost, addressPort.toInt())
        }.mapIndexed { index, grpcAddress ->
            NodeIdentifier(index.toString()) to grpcAddress
        }.toMap()

    private fun String.asLongRange(): LongRange {
        val (from, to) = this.split("..", limit = 2).map { it.toLong() }
        return from..to
    }

    private fun String.extractStatefulSetOrdinaryId(): Long = this.split(".").first()
        .split("-").last().toLong()
}