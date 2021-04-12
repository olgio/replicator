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

/**
 * Тесты реализации [DelayTimerFactory]
 */
class DelayTimerFactoryTests {

    /**
     * Срабатывание таймера с фиксированным периодом [DelayTimerFactory.fixedRateFlow]
     * Таймер с начальным периодом в 400 мс и периодом в 200 мс за 800 мс срабатывает 3 раза
     */
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

    /**
     * Срабатывание таймера с фиксированным периодом [DelayTimerFactory.fixedRateFlow]
     * Таймер с начальным периодом в 400 мс и периодом в 200 мс за 200 мс срабатывает 0 раз
     */
    @Test
    fun fixedRateFlowZeroTimeTicksTest(): Unit = runBlockingTest {
        val timerFactory = DelayTimerFactory(currentTimeTick = { TimeTick(currentTime) })

        val fixedRateFlow = timerFactory.fixedRateFlow(initialDelay = 400L, period = 200L)

        var fireCount = 0L
        val job = launch {
            fixedRateFlow.collect {
                fireCount++
            }
        }

        advanceTimeBy(200L)

        assertThat(fireCount).isEqualTo(0)
        job.cancel()
    }

    /**
     * Срабатывание таймера с фиксированным периодом [DelayTimerFactory.randomRateFlow]
     */
    @Test
    fun randomRateFlowTest(): Unit = runBlockingTest {
        val timerFactory = DelayTimerFactory(currentTimeTick = { TimeTick(currentTime) })

        val fixedRateFlow = timerFactory.randomRateFlow(initialDelay = (400L..400L), period = (200L..200L))

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

    /**
     * Срабатывание таймера,
     * реагирующего на отсутствие события в исходном потоке [DelayTimerFactory.expirationFlow]
     * 10 исходных событий с периодом в 100 мс
     * Период таймера 200 мс
     * Тогда за 1400 мс таймер срабатывает 2 раза
     */
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

    /**
     * Срабатывание таймера,
     * реагирующего на отсутствие события в исходном потоке [DelayTimerFactory.expirationFlow]
     * 1 исходное событие с задержкой в 1000 мс
     * Период таймера 200 мс
     * Тогда за 800 мс таймер срабатывает 0 раз
     */
    @Test
    fun expirationFlowZeroTimeTicksTest(): Unit = runBlockingTest {
        val timerFactory = DelayTimerFactory(currentTimeTick = { TimeTick(currentTime) })

        val flow = flow {
            delay(1000)
            emit(Unit)
        }

        val fixedRateFlow = timerFactory.expirationFlow(flow = flow, period = (200L..200L))

        var fireCount = 0L
        val job = launch {
            fixedRateFlow.collect {
                fireCount++
            }
        }

        advanceTimeBy(800L)

        assertThat(fireCount).isEqualTo(4)
        job.cancel()
    }

    /**
     * Срабатывание таймера,
     * реагирующего на исходные события и их отсутствие [DelayTimerFactory.sourceMergedExpirationFlow]
     * 10 исходных событий с периодом в 100 мс
     * Период таймера 200 мс
     * Тогда за 1400 мс таймер срабатывает 12 раз
     */
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

    /**
     * Срабатывание таймера
     * реагирующего на исходные события и их отсутствие [DelayTimerFactory.sourceMergedExpirationFlow]
     * 1 исходное событие с задержкой в 1000 мс
     * Период таймера 200 мс
     * Тогда за 800 мс таймер срабатывает 4 раза
     */
    @Test
    fun sourceMergedExpirationFlowSourceEmptyTest(): Unit = runBlockingTest {
        val timerFactory = DelayTimerFactory(currentTimeTick = { TimeTick(currentTime) })

        val flow = flow {
            delay(1000)
            emit(Unit)
        }

        val fixedRateFlow = timerFactory.sourceMergedExpirationFlow(flow = flow, period = (200L..200L))

        var fireCount = 0L
        val job = launch {
            fixedRateFlow.collect {
                fireCount++
            }
        }

        advanceTimeBy(800L)

        assertThat(fireCount).isEqualTo(4)
        job.cancel()
    }

    /**
     * Поток [DelayTimerFactory.delayedFlow], задерживающий каждое исходное сообщение
     * 2 пакета по 4 сообщения с периодом в 200 мс
     * Период задержки 200 мс
     * Тогда за 400 мс должно быть получено 8 сообщений
     */
    @Test
    fun delayedFlowTest(): Unit = runBlockingTest {
        val timerFactory = DelayTimerFactory(currentTimeTick = { TimeTick(currentTime) })

        val flow = flow {
            repeat(4) {
                emit(Unit)
            }
            delay(200)
            repeat(4) {
                emit(Unit)
            }
        }

        val delayedFlow = timerFactory.delayedFlow(flow = flow, delay = (200L..200L))

        var fireCount = 0L
        val job = launch {
            delayedFlow.collect {
                fireCount++
            }
        }

        advanceTimeBy(200L)
        assertThat(fireCount).isEqualTo(4)

        advanceTimeBy(200L)
        assertThat(fireCount).isEqualTo(8)

        job.cancel()
    }
}