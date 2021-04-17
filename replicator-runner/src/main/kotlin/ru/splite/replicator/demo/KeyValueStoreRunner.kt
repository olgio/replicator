package ru.splite.replicator.demo

import io.micrometer.core.instrument.Tag
import kotlinx.cli.ArgParser
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.runBlocking
import org.kodein.di.DI
import org.kodein.di.bind
import org.kodein.di.instance
import org.kodein.di.singleton
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import ru.splite.replicator.demo.keyvalue.KeyValueStateMachine
import ru.splite.replicator.metrics.Metrics
import ru.splite.replicator.statemachine.ConflictOrderedStateMachine
import ru.splite.replicator.timer.flow.DelayTimerFactory
import ru.splite.replicator.timer.flow.TimerFactory
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.grpc.GrpcAddress
import ru.splite.replicator.transport.grpc.GrpcTransport

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

        bind<KeyValueStoreController>() with singleton {
            KeyValueStoreController(
                config.port,
                config.nodeIdentifier,
                instance()
            )
        }

    }

    fun run() {
        val storeController: KeyValueStoreController by dependencyContainer.instance()
        storeController.start()

        LOGGER.info("Application started")

        val transport: GrpcTransport by dependencyContainer.instance()
        transport.awaitTermination()
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
                Tag.of("protocol", config.protocol.name)
            )
        )
    }

    val protocolDependencyContainer = when (config.protocol) {
        RunnerConfig.Protocol.RAFT -> RaftDependencyContainer.module
        RunnerConfig.Protocol.ATLAS -> AtlasDependencyContainer.module
        RunnerConfig.Protocol.PAXOS -> PaxosDependencyContainer.module
    }

    runBlocking(newFixedThreadPoolContext(config.threads, "node-${config.nodeIdentifier}")) {
        KeyValueStoreRunner(
            protocolDependencyContainer = protocolDependencyContainer,
            coroutineScope = this,
            config = config
        ).run()
    }
}