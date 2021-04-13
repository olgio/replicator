package ru.splite.replicator.demo

import kotlinx.coroutines.*
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Disabled
import org.kodein.di.*
import ru.splite.replicator.AtlasProtocolConfig
import ru.splite.replicator.demo.keyvalue.KeyValueCommand
import ru.splite.replicator.demo.keyvalue.KeyValueReply
import ru.splite.replicator.demo.keyvalue.KeyValueStateMachine
import ru.splite.replicator.raft.RaftProtocol
import ru.splite.replicator.raft.RaftProtocolConfig
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import ru.splite.replicator.timer.flow.DelayTimerFactory
import ru.splite.replicator.timer.flow.TimerFactory
import ru.splite.replicator.transport.CoroutineChannelTransport
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.Transport
import java.util.*
import kotlin.system.measureTimeMillis
import kotlin.test.Test

@Disabled
class Benchmark {

    sealed class Protocol {

        data class Atlas(val n: Int = 3, val f: Int = 1) : Protocol()

        data class Raft(val n: Int = 3) : Protocol()

        data class RaftOnlyLeader(val n: Int = 3) : Protocol()
    }

    data class BenchmarkConfig(
        val threadCount: Int = 30,
        val concurrency: Int = 30,
        val iterations: Int = 1000,
        val delay: (NodeIdentifier, NodeIdentifier) -> Long =
            { from: NodeIdentifier, to: NodeIdentifier ->
                50L
            },
        val generateKey: () -> String = { (1..3).random().toString() }
    )

    //300 parallel, 200 iters, 3 keys, 10 threads, delay 50 ms
    //RAFT: MIN=112 AVG=331.12645 MAX=771
    //ATLAS: MIN=125 AVG=196.19485 MAX=811

    //10 parallel, 1000 iters, 3 keys, 30 threads, delay 50 ms
    //RAFT: MIN=101 AVG=162.95 MAX=285
    //RAFT-LEADER: MIN=89 AVG=106.7007 MAX=140
    //ATLAS: MIN=50 AVG=89.5059 MAX=299

    @Test
    fun benchmark() {
        val protocol: Protocol = Protocol.Atlas(n = 3, f = 1)
        val config = BenchmarkConfig()
        println("CONFIG $config")
        println("PROTOCOL $protocol")
        when (protocol) {
            is Protocol.Atlas -> atlasBenchmark(config, protocol)
            is Protocol.Raft -> raftBenchmark(config, protocol)
            is Protocol.RaftOnlyLeader -> raftOnlyLeaderBenchmark(config, protocol)
        }
    }


    @Test
    fun atlasBenchmark(config: BenchmarkConfig, protocol: Protocol.Atlas) =
        runBlocking(newFixedThreadPoolContext(config.threadCount, "cluster")) {
            val transport = CoroutineChannelTransport(this, config.delay)
            val n = protocol.n
            val nodes = (1..n).map {
                val config = AtlasProtocolConfig(
                    address = NodeIdentifier("node-$it"),
                    processId = it.toLong(),
                    n = n,
                    f = protocol.f
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

            startBenchmark(config, nodes)
            assertStateMachines(nodes)

            coroutineContext.job.children.forEach {
                it.cancel()
            }
        }

    @Test
    fun raftBenchmark(config: BenchmarkConfig, protocol: Protocol.Raft) =
        runBlocking(newFixedThreadPoolContext(config.threadCount, "cluster")) {
            val transport = CoroutineChannelTransport(this, config.delay)
            val n = protocol.n
            val nodes = (1..n).map {
                val config = RaftProtocolConfig(
                    address = NodeIdentifier(it.toString()),
                    n = n,
                    termClockPeriod = 3000L..5000L,
                    appendEntriesSendPeriod = 1000L..1000L
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

            startBenchmark(config, nodes)
            assertStateMachines(nodes)

            coroutineContext.job.children.forEach {
                it.cancel()
            }
        }

    @Test
    fun raftOnlyLeaderBenchmark(config: BenchmarkConfig, protocol: Protocol.RaftOnlyLeader) =
        runBlocking(newFixedThreadPoolContext(config.threadCount, "cluster")) {
            val transport = CoroutineChannelTransport(this)
            val n = protocol.n
            val nodes = (1..n).map {
                val config = RaftProtocolConfig(
                    address = NodeIdentifier(it.toString()),
                    n = n,
                    termClockPeriod = 3000L..5000L,
                    appendEntriesSendPeriod = 1000L..1000L
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

            startBenchmark(config, nodes.filter { it.instance<RaftProtocol>().isLeader })
            assertStateMachines(nodes)

            coroutineContext.job.children.forEach {
                it.cancel()
            }
        }

    private suspend fun startBenchmark(config: BenchmarkConfig, nodes: List<DirectDI>) = coroutineScope {
        val submitters = nodes.map { it.instance<StateMachineCommandSubmitter<ByteArray, ByteArray>>() }


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
                            assertThat(commandReply.value).isEqualTo(value)
                        }
                    }
                    //println("$id) TIME=$time")
                    time
                }
            }
        }.flatMap { it.await() }

        val fullTime = System.currentTimeMillis() - start
        println("REPORT:")
        println("  MIN=${times.minOrNull()} AVG=${times.average()} MAX=${times.maxOrNull()}")
        println("  FULLTIME=$fullTime COUNT=${times.size} TPS=${times.size / (fullTime / 1000)}")

        delay(5000L)
    }

    private suspend fun assertStateMachines(nodes: List<DirectDI>) = coroutineScope {
        val allKeys = nodes.flatMap { it.instance<KeyValueStateMachine>().currentState.keys }.toSet()
        assertThat(allKeys).isNotEmpty
        allKeys.forEach { key ->
            assertThat(nodes.map {
                it.instance<KeyValueStateMachine>().currentState[key]
            }.toSet()).hasSize(1)
        }

        println("Completed with ${allKeys.size} keys")
    }
}