package ru.splite.replicator.demo

import io.micrometer.core.instrument.Tag
import kotlinx.cli.ArgParser
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.runBlocking
import org.kodein.di.DI
import org.kodein.di.bind
import org.kodein.di.instance
import org.kodein.di.singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.splite.replicator.atlas.graph.Dependency
import ru.splite.replicator.demo.keyvalue.KeyValueConflictIndex
import ru.splite.replicator.demo.keyvalue.KeyValueStateMachine
import ru.splite.replicator.metrics.Metrics
import ru.splite.replicator.statemachine.ConflictIndex
import ru.splite.replicator.statemachine.ConflictOrderedStateMachine
import ru.splite.replicator.timer.flow.DelayTimerFactory
import ru.splite.replicator.timer.flow.TimerFactory
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.grpc.GrpcAddress
import ru.splite.replicator.transport.grpc.GrpcTransport
import java.net.InetAddress

class KeyValueStoreRunner(
    protocolDependencyContainer: DI.Module,
    coroutineScope: CoroutineScope,
    config: RunnerConfig
) {

    private val dependencyContainer = DI {

        import(protocolDependencyContainer)

        bind<RunnerConfig>() with instance(config)

        bind<CoroutineScope>() with instance(coroutineScope)

        bind<TimerFactory>() with singleton { DelayTimerFactory() }

        bind<Map<NodeIdentifier, GrpcAddress>>() with instance(config.nodes)

        bind<GrpcTransport>() with singleton { GrpcTransport(instance()) }

        bind<ConflictOrderedStateMachine<ByteArray, ByteArray>>() with singleton { KeyValueStateMachine() }

        bind<ConflictIndex<Dependency, ByteArray>>() with singleton { KeyValueConflictIndex() }

        bind<KeyValueStoreController>() with singleton {
            KeyValueStoreController(
                config.port,
                config.nodeIdentifier,
                config.maxConcurrentSubmits,
                instance(),
                Dispatchers.Default
            )
        }

        if (config.rocksDbFile != null) {
            import(RocksDbDependencyContainer.module, allowOverride = true)
        }
    }

    fun run(): GrpcTransport {
        val storeController: KeyValueStoreController by dependencyContainer.instance()
        storeController.start()

        LOGGER.info("Application started")

        val transport: GrpcTransport by dependencyContainer.instance()
        return transport
    }

    companion object {
        val LOGGER: Logger = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}

fun main(args: Array<String>) {
    val parser = ArgParser("replicator")

    val config = RunnerConfig(parser)

    parser.parse(args)

    config.googleProjectId?.let { googleProjectId ->
        Metrics.initializeStackdriver(
            googleProjectId, listOf(
                Tag.of("protocol", config.protocol.name),
                Tag.of("pod", InetAddress.getLocalHost().hostName)
            )
        )
    }

    val protocolDependencyContainer = when (config.protocol) {
        RunnerConfig.Protocol.RAFT -> RaftDependencyContainer.module
        RunnerConfig.Protocol.ATLAS -> AtlasDependencyContainer.module
        RunnerConfig.Protocol.PAXOS -> PaxosDependencyContainer.module
    }

    val rootCoroutineContext = if (config.threads > 0) {
        newFixedThreadPoolContext(config.threads, "node-${config.nodeIdentifier}")
    } else Dispatchers.Default

    val transport = runBlocking(rootCoroutineContext) {
        KeyValueStoreRunner(
            protocolDependencyContainer = protocolDependencyContainer,
            coroutineScope = this,
            config = config
        ).run()
    }
    transport.awaitTermination()
}