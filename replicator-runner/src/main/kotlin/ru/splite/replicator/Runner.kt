package ru.splite.replicator

import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.default
import kotlinx.cli.required
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.runBlocking
import org.kodein.di.DI
import org.kodein.di.bind
import org.kodein.di.instance
import org.kodein.di.singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.executor.CommandExecutor
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.graph.DependencyGraph
import ru.splite.replicator.graph.JGraphTDependencyGraph
import ru.splite.replicator.id.IdGenerator
import ru.splite.replicator.id.InMemoryIdGenerator
import ru.splite.replicator.keyvalue.KeyValueStateMachine
import ru.splite.replicator.statemachine.ConflictIndex
import ru.splite.replicator.statemachine.StateMachine
import ru.splite.replicator.transport.grpc.GrpcAddress
import ru.splite.replicator.transport.grpc.GrpcTransport
import ru.splite.replicator.transport.sender.MessageSender
import java.net.InetAddress

class Runner(
    private val coroutineScope: CoroutineScope,
    private val nodes: Map<NodeIdentifier, GrpcAddress>,
    private val config: AtlasProtocolConfig,
    private val port: Int
) {

    private val dependencyContainer = DI {

        bind<CoroutineScope>() with instance(coroutineScope)

        bind<AtlasProtocolConfig>() with instance(config)
        bind<Map<NodeIdentifier, GrpcAddress>>() with instance(nodes)

        bind<GrpcTransport>() with singleton { GrpcTransport(instance()) }

        bind<StateMachine<ByteArray, ByteArray>>() with singleton { KeyValueStateMachine() }
        bind<KeyValueCommandSubmitController>() with singleton {
            KeyValueCommandSubmitController(
                port,
                instance(),
                instance()
            )
        }
        bind<ConflictIndex<Dependency, ByteArray>>() with singleton {
            instance<StateMachine<ByteArray, ByteArray>>().newConflictIndex()
        }

        bind<MessageSender<AtlasMessage>>() with singleton {
            MessageSender(
                instance(),
                instance<AtlasProtocolConfig>().sendMessageTimeout
            )
        }

        bind<IdGenerator<NodeIdentifier>>() with singleton {
            InMemoryIdGenerator(instance<AtlasProtocolConfig>().address)
        }

        bind<DependencyGraph<Dependency>>() with singleton { JGraphTDependencyGraph() }

        bind<CommandExecutor>() with singleton { CommandExecutor(instance(), instance()) }

        bind<BaseAtlasProtocol>() with singleton { BaseAtlasProtocol(instance(), instance(), instance(), instance()) }

        bind<AtlasProtocolController>() with singleton { AtlasProtocolController(instance(), instance()) }

        bind<AtlasCommandSubmitter>() with singleton {
            AtlasCommandSubmitter(
                instance(),
                instance(),
                instance(),
                instance()
            )
        }

    }

    fun run() {
        val config: AtlasProtocolConfig by dependencyContainer.instance()
        val nodes: Map<NodeIdentifier, GrpcAddress> by dependencyContainer.instance()
        val commandExecutor: CommandExecutor by dependencyContainer.instance()
        val transport: GrpcTransport by dependencyContainer.instance()
        val commandSubmitController: KeyValueCommandSubmitController by dependencyContainer.instance()

        val commandExecutorJob = commandExecutor.launchCommandExecutor(CoroutineName("executor"), coroutineScope)

        commandSubmitController.start()

        LOGGER.info("Application started with config $config, nodes $nodes")
        transport.awaitTermination()
    }

    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}

fun main(args: Array<String>) {
    val parser = ArgParser("replicator")

    val f by parser
        .option(type = ArgType.Int, description = "Count of possible failures")
        .default(1)

    val nodes by parser
        .option(type = ArgType.String, description = "List of nodes")
        .required()

    val messageTimeout by parser
        .option(type = ArgType.Int, description = "Message timeout ms")
        .default(1000 * 15)

    val commandExecutorTimeout by parser
        .option(type = ArgType.Int, description = "Command execution timeout ms")
        .default(1000 * 15)

    val processId by parser.option(type = ArgType.Int, description = "Process id")

    val port by parser
        .option(type = ArgType.Int, description = "Port")
        .default(8000)

    parser.parse(args)

    val addresses: List<GrpcAddress> = nodes.split(",").map {
        val (addressHost, addressPort) = it.split(":", limit = 2)
        GrpcAddress(addressHost, addressPort.toInt())
    }

    val addressMap: Map<NodeIdentifier, GrpcAddress> = addresses.mapIndexed { index, grpcAddress ->
        NodeIdentifier(index.toString()) to grpcAddress
    }.toMap()

    val processIdExplicit = processId ?: InetAddress.getLocalHost().hostName.asProcessId()

    val nodeIdentifier = NodeIdentifier(processIdExplicit.toString())

    val config = AtlasProtocolConfig(
        address = nodeIdentifier,
        processId = processIdExplicit.toLong(),
        sendMessageTimeout = messageTimeout.toLong(),
        commandExecutorTimeout = commandExecutorTimeout.toLong(),
        n = addressMap.size,
        f = f
    )

    runBlocking(newFixedThreadPoolContext(10, "node-$nodeIdentifier")) {
        val runner = Runner(this, addressMap, config, port)
        runner.run()
    }
}

private fun String.asProcessId(): Long = this
    .split(".")
    .first()
    .split("-")
    .last()
    .toLong()