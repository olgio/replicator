package ru.splite.replicator.raft.timer

import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.schedule

class RenewableTimerFactory {

    private val timer: Timer = Timer("renewable-timer")

    fun scheduleWithPeriodAndInitialDelay(periodMillis: Long, action: TimerTask.() -> Unit): RenewableTimerTask {
        val timerTask = RenewableTimerTaskImpl(this.timer, periodMillis, action)
        timerTask.start()
        return timerTask
    }

    private class RenewableTimerTaskImpl(
        private val timer: Timer,
        private val periodMillis: Long,
        private val action: TimerTask.() -> Unit
    ) : RenewableTimerTask {

        private val runAfter: AtomicLong = AtomicLong(evaluateRenewedTime())

        private val isActive: AtomicBoolean = AtomicBoolean(true)

        override fun renew() {
            val newRunAfter = evaluateRenewedTime()
            runAfter.updateAndGet {
                if (it < newRunAfter) newRunAfter else it
            }
        }

        override fun cancel() {
            isActive.set(false)
        }

        fun start() {
            timer.schedule(Date(runAfter.get())) {
                if (!isActive.get()) {
                    return@schedule
                }
                try {
                    this.executeIfExpired()
                } finally {
                    renew()
                    start()
                }
            }
        }

        private fun evaluateRenewedTime(): Long = System.currentTimeMillis() + periodMillis

        private fun TimerTask.executeIfExpired() {
            if (System.currentTimeMillis() >= runAfter.get()) {
                action()
            }
        }
    }
}