package ru.splite.replicator.metrics

import com.google.common.base.Stopwatch
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tag
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import io.micrometer.stackdriver.StackdriverConfig
import io.micrometer.stackdriver.StackdriverMeterRegistry
import java.time.Duration
import java.util.concurrent.TimeUnit

object Metrics {

    private var meterRegistryBuildAction: () -> MeterRegistry = {
        SimpleMeterRegistry()
    }

    val registry: MetricsRegistry by lazy {
        MetricsRegistry(meterRegistryBuildAction())
    }

    class MetricsRegistry(micrometerRegistry: MeterRegistry) {

        val commandSubmitLatency: Timer = buildTimer("replicator.submit.latency")
            .register(micrometerRegistry)

        val commandSubmitErrorLatency: Timer = buildTimer("replicator.submit.error.latency")
            .register(micrometerRegistry)

        val sendMessageLatency: Timer = buildTimer("replicator.transport.send.latency")
            .register(micrometerRegistry)

        val receiveMessageLatency: Timer = buildTimer("replicator.transport.receive.latency")
            .register(micrometerRegistry)

        val atlasCommandExecutorLatency: Timer = buildTimer("replicator.atlas.executor.latency")
            .register(micrometerRegistry)

        val atlasCommandCoordinateLatency: Timer = buildTimer("replicator.atlas.coordinator.latency")
            .register(micrometerRegistry)

        val atlasRecoveryCounter: Counter =
            micrometerRegistry.counter("replicator.atlas.recovery.count")

        val atlasFastPathCounter: Counter =
            micrometerRegistry.counter("replicator.atlas.fastpath.count")

        val atlasSlowPathCounter: Counter =
            micrometerRegistry.counter("replicator.atlas.slowpath.count")

        val rocksDbWriteLatency: Timer = buildTimer("replicator.rocksdb.write.latency")
            .register(micrometerRegistry)

        val rocksDbReadLatency: Timer = buildTimer("replicator.rocksdb.read.latency")
            .register(micrometerRegistry)
    }

    private fun buildTimer(name: String): Timer.Builder = Timer
        .builder(name)
        .minimumExpectedValue(Duration.ofMillis(1))
        .maximumExpectedValue(Duration.ofSeconds(30))
        .publishPercentiles(0.05, 0.5, 0.95, 0.99)
        .publishPercentileHistogram()

    fun initializeStackdriver(projectId: String, tags: List<Tag>) {
        this.meterRegistryBuildAction = {
            val meterRegistry = StackdriverMeterRegistry.builder(object : StackdriverConfig {
                override fun projectId(): String {
                    return projectId
                }

                override fun get(p0: String): String? {
                    return null
                }
            }).build()
            meterRegistry.config().commonTags(tags)
            meterRegistry
        }
        //force initialize lazy property
        registry
    }

    fun Timer.recordStopwatch(stopwatch: Stopwatch) {
        this.record(stopwatch.elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
    }

    inline fun <T> Timer.measureAndRecord(action: () -> T): T {
        val stopwatch = Stopwatch.createStarted()
        try {
            return action()
        } finally {
            this.recordStopwatch(stopwatch.stop())
        }
    }
}