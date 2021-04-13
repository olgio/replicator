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

    var registry: MetricsRegistry = MetricsRegistry(SimpleMeterRegistry())

    class MetricsRegistry(private val appMicrometerRegistry: MeterRegistry) {

        val commandSubmitLatency: Timer = Timer
            .builder("replicator.submit.latency")
            .minimumExpectedValue(Duration.ofMillis(1))
            .maximumExpectedValue(Duration.ofSeconds(30))
            .publishPercentiles(0.05, 0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(appMicrometerRegistry)

        val commandSubmitErrorLatency: Timer = Timer
            .builder("replicator.submit.error.latency")
            .minimumExpectedValue(Duration.ofMillis(1))
            .maximumExpectedValue(Duration.ofSeconds(30))
            .publishPercentiles(0.05, 0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(appMicrometerRegistry)

        val sendMessageLatency: Timer = Timer
            .builder("replicator.transport.send.latency")
            .minimumExpectedValue(Duration.ofMillis(1))
            .maximumExpectedValue(Duration.ofSeconds(30))
            .publishPercentiles(0.05, 0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(appMicrometerRegistry)

        val receiveMessageLatency: Timer = Timer
            .builder("replicator.transport.receive.latency")
            .minimumExpectedValue(Duration.ofMillis(1))
            .maximumExpectedValue(Duration.ofSeconds(30))
            .publishPercentiles(0.05, 0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(appMicrometerRegistry)

        val atlasCommandExecutorLatency: Timer = Timer
            .builder("replicator.atlas.executor.latency")
            .minimumExpectedValue(Duration.ofMillis(1))
            .maximumExpectedValue(Duration.ofSeconds(30))
            .publishPercentiles(0.05, 0.5, 0.95, 0.99)
            .publishPercentileHistogram()
            .register(appMicrometerRegistry)

        val atlasRecoveryCounter: Counter =
            appMicrometerRegistry.counter("replicator.atlas.recovery.count")
    }

    fun initializeStackdriver(projectId: String, tags: List<Tag>) {
        val meterRegistry = StackdriverMeterRegistry.builder(object : StackdriverConfig {
            override fun projectId(): String {
                return projectId
            }

            override fun get(p0: String): String? {
                return null
            }
        }).build()
        meterRegistry.config().commonTags(tags)
        this.registry = MetricsRegistry(meterRegistry)
    }

    fun Timer.recordStopwatch(stopwatch: Stopwatch) {
        this.record(stopwatch.elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
    }

    inline fun Timer.measureAndRecord(action: () -> Unit) {
        val stopwatch = Stopwatch.createStarted()
        try {
            action()
        } finally {
            this.record(stopwatch.stop().elapsed(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS)
        }
    }
}