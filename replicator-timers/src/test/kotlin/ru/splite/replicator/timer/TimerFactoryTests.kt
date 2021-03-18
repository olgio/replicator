package ru.splite.replicator.timer

import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.timer.flow.DelayTimerFactory
import ru.splite.replicator.timer.flow.TimeTick
import kotlin.test.Test

class TimerFactoryTests {

    @Test
    fun fixedRateFlowTest(): Unit = runBlockingTest {
        val timerFactory = DelayTimerFactory(currentTimeTick = { TimeTick(currentTime) })

        val fixedRateFlow = timerFactory.fixedRateFlow(initialDelay = 400L, period = 200L)

        var fireCount = 0L
        val job = launch {
            fixedRateFlow.collect {
                fireCount++
            }
        }

        advanceTimeBy(800L)

        assertThat(fireCount).isEqualTo(3)
        job.cancel()
    }

    @Test
    fun expirationFlowTest(): Unit = runBlockingTest {
        val timerFactory = DelayTimerFactory(currentTimeTick = { TimeTick(currentTime) })

        val flow = flow {
            repeat(10) {
                emit(Unit)
                delay(100)
            }
        }

        val fixedRateFlow = timerFactory.expirationFlow(flow = flow, period = (200L..200L))

        var fireCount = 0L
        val job = launch {
            fixedRateFlow.collect {
                fireCount++
            }
        }

        advanceTimeBy(1400L)

        assertThat(fireCount).isEqualTo(2)
        job.cancel()
    }

    @Test
    fun sourceMergedExpirationFlowTest(): Unit = runBlockingTest {
        val timerFactory = DelayTimerFactory(currentTimeTick = { TimeTick(currentTime) })

        val flow = flow {
            repeat(10) {
                emit(Unit)
                delay(100)
            }
        }

        val fixedRateFlow = timerFactory.sourceMergedExpirationFlow(flow = flow, period = (200L..200L))

        var fireCount = 0L
        val job = launch {
            fixedRateFlow.collect {
                fireCount++
            }
        }

        advanceTimeBy(1400L)

        assertThat(fireCount).isEqualTo(12)
        job.cancel()
    }
}