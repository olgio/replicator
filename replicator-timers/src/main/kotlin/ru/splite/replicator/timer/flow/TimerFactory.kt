package ru.splite.replicator.timer.flow

import kotlinx.coroutines.flow.Flow

interface TimerFactory {

    /**
     * Таймер с фиксированной задержкой [initialDelay] мс и периодом [period] мс
     */
    fun fixedRateFlow(name: String? = null, initialDelay: Long = 0L, period: Long): Flow<TimeTick>

    /**
     * Таймер с случайной задержкой [initialDelay] мс и периодом [period] мс
     */
    fun randomRateFlow(name: String? = null, initialDelay: LongRange, period: LongRange): Flow<TimeTick>

    /**
     * Таймер, реагирующий на отсутствие сообщений в исходном потоке [flow] более периода времени [period] мс
     */
    fun expirationFlow(name: String? = null, flow: Flow<*>, period: LongRange): Flow<TimeTick>

    /**
     * Таймер, реагирующий как на поступление событий в исходный поток [flow], так и на их отсутствие (аналогично [expirationFlow]
     */
    fun sourceMergedExpirationFlow(name: String? = null, flow: Flow<*>, period: LongRange): Flow<TimeTick>

    /**
     * Формирует новый поток их исходного потока [flow], где каждое сообщение задерживается на [delay] мс
     */
    fun <T> delayedFlow(flow: Flow<T>, delay: LongRange): Flow<T>
}