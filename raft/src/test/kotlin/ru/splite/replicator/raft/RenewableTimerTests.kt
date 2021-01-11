package ru.splite.replicator.raft

import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions
import ru.splite.replicator.raft.timer.RenewableTimerFactory
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.Test

class RenewableTimerTests {

    private val renewableTimerFactory = RenewableTimerFactory()

    @Test
    fun expiredTimeTest(): Unit = runBlocking {
        val counter = AtomicInteger(0)
        renewableTimerFactory.scheduleWithPeriodAndInitialDelay(1000) {
            counter.incrementAndGet()
        }
        delay(2500)
        Assertions.assertThat(counter.get()).isEqualTo(2)
    }

    @Test
    fun renewTest(): Unit = runBlocking {
        val counter = AtomicInteger(0)
        val timerTask = renewableTimerFactory.scheduleWithPeriodAndInitialDelay(1000) {
            counter.incrementAndGet()
        }
        repeat(5) {
            delay(500)
            timerTask.renew()
        }
        Assertions.assertThat(counter.get()).isEqualTo(0)
        delay(1500)
        Assertions.assertThat(counter.get()).isEqualTo(1)
    }

    @Test
    fun cancelTest(): Unit = runBlocking {
        val counter = AtomicInteger(0)
        val timerTask = renewableTimerFactory.scheduleWithPeriodAndInitialDelay(1000) {
            counter.incrementAndGet()
        }
        timerTask.cancel()
        delay(1500)
        Assertions.assertThat(counter.get()).isEqualTo(0)
    }

}