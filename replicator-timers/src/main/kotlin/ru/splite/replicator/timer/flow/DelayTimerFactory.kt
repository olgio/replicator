package ru.splite.replicator.timer.flow

import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.*
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random
import kotlin.random.nextLong

class DelayTimerFactory(
    private val random: Random = Random,
    private val currentTimeTick: () -> TimeTick = { TimeTick(System.currentTimeMillis()) }
) : TimerFactory {

    override fun fixedRateFlow(name: String?, initialDelay: Long, period: Long): Flow<TimeTick> {
        return randomRateFlow(name, (initialDelay..initialDelay), (period..period))
    }

    override fun randomRateFlow(name: String?, initialDelay: LongRange, period: LongRange): Flow<TimeTick> {
        return flow {
            delay(random.nextLong(initialDelay))
            while (true) {
                emit(currentTimeTick())
                delay(random.nextLong(period))
            }
        }
    }

    override fun expirationFlow(name: String?, flow: Flow<*>, period: LongRange): Flow<TimeTick> {
        val renewFlow = flow.map {
            currentTimeTick()
        }

        return flow {
            val runAt = AtomicLong(currentTimeTick().tick + random.nextLong(period))
            coroutineScope {
                launch {
                    renewFlow.collect { renewTimeTick ->
                        val newValue = renewTimeTick.tick + random.nextLong(period)
                        runAt.updateIfGreater(newValue)
                    }
                }

                while (true) {
                    delayBefore(TimeTick(runAt.get()))
                    val currentTimeTick = currentTimeTick()
                    LOGGER.trace("currentTime = ${currentTimeTick.tick}, runAt = ${runAt.get()}")
                    if (runAt.get() <= currentTimeTick.tick) {
                        emit(currentTimeTick)
                        runAt.updateIfGreater(currentTimeTick.tick + random.nextLong(period))
                    }
                }
            }
        }.conflate()
    }

    override fun sourceMergedExpirationFlow(name: String?, flow: Flow<*>, period: LongRange): Flow<TimeTick> {
        val renewFlow = flow.map {
            currentTimeTick()
        }

        val expiringFlow = expirationFlow(name, flow, period)

        return flowOf(renewFlow, expiringFlow).flattenMerge().conflate()
    }

    override fun <T> delayedFlow(flow: Flow<T>, delay: LongRange): Flow<T> {

        data class ValueWithTimeTick(val value: T, val timeTick: TimeTick)

        return flow.map {
            ValueWithTimeTick(it, TimeTick(currentTimeTick().tick + delay.random(random)))
        }.buffer(capacity = Channel.UNLIMITED).map {
            delayBefore(it.timeTick)
            it.value
        }
    }

    private suspend fun delayBefore(timeTick: TimeTick) {
        val diff = timeTick.tick - currentTimeTick().tick
        LOGGER.trace("Delay for $diff ms")
        if (diff <= 0) {
            return
        } else {
            delay(diff)
        }
    }

    private fun AtomicLong.updateIfGreater(newValue: Long): Long {
        return this.updateAndGet {
            if (it < newValue) newValue else it
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}