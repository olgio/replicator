package ru.splite.replicator.benchmark

import com.google.common.base.Stopwatch
import kotlinx.cli.ArgParser
import kotlinx.cli.ArgType
import kotlinx.cli.default
import kotlinx.coroutines.*
import org.kodein.di.*
import ru.splite.replicator.AtlasProtocolConfig
import ru.splite.replicator.demo.AtlasDependencyContainer
import ru.splite.replicator.demo.RaftDependencyContainer
import ru.splite.replicator.demo.keyvalue.KeyValueCommand
import ru.splite.replicator.demo.keyvalue.KeyValueReply
import ru.splite.replicator.demo.keyvalue.KeyValueStateMachine
import ru.splite.replicator.metrics.Metrics
import ru.splite.replicator.raft.RaftProtocol
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.raft.state.asMajority
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import ru.splite.replicator.timer.flow.DelayTimerFactory
import ru.splite.replicator.timer.flow.TimerFactory
import ru.splite.replicator.transport.CoroutineChannelTransport
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.Transport
import java.util.*
import java.util.concurrent.TimeUnit
import kotlin.math.ceil
import kotlin.system.measureTimeMillis


class Benchmark {

    sealed class Protocol(open val n: Int, open val f: Int) {

        data class Atlas(override val n: Int = 3, override val f: Int = 1) : Protocol(n, f)

        data class Raft(override val n: Int = 3) : Protocol(n, n - n.asMajority())

        data class RaftOnlyLeader(override val n: Int = 3) : Protocol(n, n - n.asMajority())
    }

    data class BenchmarkConfig(
        val threadCount: Int = 30,
        val concurrency: Int = 30,
        val iterations: Int = 1000,
        val delayMs: Int = 50,
        val keys: Int = 3
    ) {
        val generateKey: () -> String = { (1..keys).random().toString() }

        val delay: (NodeIdentifier, NodeIdentifier) -> Long =
            { from: NodeIdentifier, to: NodeIdentifier ->
                delayMs.toLong()
            }
    }

    //300 parallel, 200 iters, 3 keys, 10 threads, delay 50 ms
    //RAFT: MIN=112 AVG=331.12645 MAX=771
    //ATLAS: MIN=125 AVG=196.19485 MAX=811

    //10 parallel, 1000 iters, 3 keys, 30 threads, delay 50 ms
    //RAFT: MIN=101 AVG=162.95 MAX=285
    //RAFT-LEADER: MIN=89 AVG=106.7007 MAX=140
    //ATLAS: MIN=50 AVG=89.5059 MAX=299

    fun atlasBenchmark(config: BenchmarkConfig, protocol: Protocol.Atlas) =
        runBlocking(newFixedThreadPoolContext(config.threadCount, "cluster")) {
            val transport = CoroutineChannelTransport(this, config.delay)
            val n = protocol.n
            val nodes = (1..n).map {
                val config = AtlasProtocolConfig(
                    address = NodeIdentifier("node-$it"),
                    processId = it.toLong(),
                    n = n,
                    f = protocol.f,
                    enableRecovery = false,
                    sendMessageTimeout = 10000L,
                    commandExecutorTimeout = 10000L
                )

                DI {
                    import(AtlasDependencyContainer.module, allowOverride = true)

                    bind<AtlasProtocolConfig>(overrides = true) with instance(config)

                    bind<CoroutineScope>() with instance(this@runBlocking)

                    bind<TimerFactory>() with singleton { DelayTimerFactory() }

                    bind<Transport>() with instance(transport)

                    bind<KeyValueStateMachine>() with singleton { KeyValueStateMachine() }
                }.direct
            }

            delay(2000)

            startBenchmark(config, protocol, nodes)
            assertStateMachines(nodes)

            coroutineContext.job.children.forEach {
                it.cancel()
            }
        }

    fun raftBenchmark(config: BenchmarkConfig, protocol: Protocol.Raft) =
        runBlocking(newFixedThreadPoolContext(config.threadCount, "cluster")) {
            val transport = CoroutineChannelTransport(this, config.delay)
            val n = protocol.n
            val nodes = (1..n).map {
                val config = RaftProtocolConfig(
                    address = NodeIdentifier(it.toString()),
                    n = n,
                    termClockPeriod = 3000L..5000L,
                    appendEntriesSendPeriod = 1000L..1000L,
                    sendMessageTimeout = 10000L,
                    commandExecutorTimeout = 10000L
                )

                DI {
                    import(RaftDependencyContainer.module, allowOverride = true)

                    bind<RaftProtocolConfig>(overrides = true) with instance(config)

                    bind<CoroutineScope>() with instance(this@runBlocking)

                    bind<TimerFactory>() with singleton { DelayTimerFactory() }

                    bind<Transport>() with instance(transport)

                    bind<KeyValueStateMachine>() with singleton { KeyValueStateMachine() }
                }.direct
            }

            nodes.map { it.instance<StateMachineCommandSubmitter<ByteArray, ByteArray>>() }
            val protocols = nodes.map { it.instance<RaftProtocol>() }
            while (protocols.none { it.isLeader }) {
                delay(200)
            }
            delay(2000)

            startBenchmark(config, protocol, nodes)
            assertStateMachines(nodes)

            coroutineContext.job.children.forEach {
                it.cancel()
            }
        }

    fun raftOnlyLeaderBenchmark(config: BenchmarkConfig, protocol: Protocol.RaftOnlyLeader) =
        runBlocking(newFixedThreadPoolContext(config.threadCount, "cluster")) {
            val transport = CoroutineChannelTransport(this, config.delay)
            val n = protocol.n
            val nodes = (1..n).map {
                val config = RaftProtocolConfig(
                    address = NodeIdentifier(it.toString()),
                    n = n,
                    termClockPeriod = 3000L..5000L,
                    appendEntriesSendPeriod = 1000L..1000L,
                    sendMessageTimeout = 10000L,
                    commandExecutorTimeout = 10000L
                )

                DI {
                    import(RaftDependencyContainer.module, allowOverride = true)

                    bind<RaftProtocolConfig>(overrides = true) with instance(config)

                    bind<CoroutineScope>() with instance(this@runBlocking)

                    bind<TimerFactory>() with singleton { DelayTimerFactory() }

                    bind<Transport>() with instance(transport)

                    bind<KeyValueStateMachine>() with singleton { KeyValueStateMachine() }
                }.direct
            }

            nodes.map { it.instance<StateMachineCommandSubmitter<ByteArray, ByteArray>>() }
            val protocols = nodes.map { it.instance<RaftProtocol>() }
            while (protocols.none { it.isLeader }) {
                delay(200)
            }
            delay(2000)

            startBenchmark(config, protocol, nodes.filter { it.instance<RaftProtocol>().isLeader })
            assertStateMachines(nodes)

            coroutineContext.job.children.forEach {
                it.cancel()
            }
        }

    private suspend fun startBenchmark(config: BenchmarkConfig, protocol: Protocol, nodes: List<DirectDI>) =
        coroutineScope {
            val submitters = nodes.map { it.instance<StateMachineCommandSubmitter<ByteArray, ByteArray>>() }

            val stopwatch = Stopwatch.createStarted()
            println("Starting warmup")

            repeat(3) {
                submitters.forEach { submitter ->
                    val value = UUID.randomUUID().toString()
                    KeyValueCommand.newPutCommand(config.generateKey(), value).let { command ->
                        val commandReply = KeyValueReply.deserializer(submitter.submit(command))
                        check(commandReply.value == value)
                    }
                }
            }
            println("Completed warmup in ${stopwatch.stop().elapsed(TimeUnit.MILLISECONDS)}ms")

            println("Starting benchmark")

            val start = System.currentTimeMillis()
            val times = (1..config.concurrency).map { i ->
                async {
                    (1..config.iterations).map { j ->
                        val id = i to j
                        //println("$id) Start")
                        val value = UUID.randomUUID().toString()
                        val time = KeyValueCommand.newPutCommand(config.generateKey(), value).let { command ->
                            measureTimeMillis {
                                val submitter = submitters.random()
                                val commandReply = KeyValueReply.deserializer(submitter.submit(command))
                                check(commandReply.value == value)
                            }
                        }
                        //println("$id) TIME=$time")
                        time
                    }
                }
            }.flatMap { it.await() }

            val fullTime = System.currentTimeMillis() - start


            println("REPORT:")
            println("  MIN=${times.minOrNull()} ms AVG=${times.average()} ms MAX=${times.maxOrNull()} ms")
            println("  5%=${times.percentile(0.05)}ms 50%=${times.percentile(0.5)}ms")
            println("  95%=${times.percentile(0.95)}ms 99%=${times.percentile(0.99)}ms")
            println("  FULLTIME=$fullTime ms COUNT=${times.size} TPS=${times.size / (fullTime / 1000)}")

            val results = listOf(
                protocol.javaClass.simpleName,
                protocol.n,
                protocol.f,
                config.keys,
                config.threadCount,
                config.concurrency,
                config.iterations,
                config.delayMs,
                times.minOrNull()?.toLong(),
                times.average().toLong(),
                times.maxOrNull()?.toLong(),
                times.percentile(0.05),
                times.percentile(0.5),
                times.percentile(0.95),
                times.percentile(0.99),
                times.size / (fullTime / 1000),
//                Metrics.registry.commandSubmitLatency.count(),
//                Metrics.registry.commandSubmitErrorLatency.count(),
                Metrics.registry.atlasFastPathCounter.count().toLong(),
                Metrics.registry.atlasSlowPathCounter.count().toLong()
            )
            println("RESULT: ${results.joinToString(", ") { it.toString() }}")

            delay(5000L)
        }

    private suspend fun assertStateMachines(nodes: List<DirectDI>) = coroutineScope {
        val allKeys = nodes.flatMap { it.instance<KeyValueStateMachine>().currentState.keys }.toSet()
        check(allKeys.isNotEmpty())
        allKeys.forEach { key ->
            check(nodes.map {
                it.instance<KeyValueStateMachine>().currentState[key]
            }.toSet().size == 1)
        }

        println("Completed with ${allKeys.size} keys")
    }

    private fun List<Long>.percentile(percentile: Double): Long {
        val index = ceil(percentile * this.size).toInt()
        return this.sorted()[index - 1]
    }
}

fun main(args: Array<String>) {
    val parser = ArgParser("benchmark")

    val protocolString by parser.option(ArgType.String, "protocol").default("Atlas")
    val n by parser.option(ArgType.Int).default(3)
    val f by parser.option(ArgType.Int).default(1)
    val keys by parser.option(ArgType.Int).default(3)
    val threads by parser.option(ArgType.Int).default(5)

    parser.parse(args)

    val benchmark = Benchmark()
    val protocol: Benchmark.Protocol = when (protocolString) {
        "Atlas" -> Benchmark.Protocol.Atlas(n = n, f = f)
        "Raft" -> Benchmark.Protocol.Raft(n = n)
        "RaftOnlyLeader" -> Benchmark.Protocol.RaftOnlyLeader(n = n)
        else -> error("No protocol $protocolString")
    }
    val config = Benchmark.BenchmarkConfig(keys = keys, threadCount = threads)
    println("CONFIG $config")
    println("PROTOCOL $protocol")
    when (protocol) {
        is Benchmark.Protocol.Atlas -> benchmark.atlasBenchmark(config, protocol)
        is Benchmark.Protocol.Raft -> benchmark.raftBenchmark(config, protocol)
        is Benchmark.Protocol.RaftOnlyLeader -> benchmark.raftOnlyLeaderBenchmark(config, protocol)
    }
}