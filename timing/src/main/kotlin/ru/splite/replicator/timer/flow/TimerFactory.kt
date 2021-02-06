package ru.splite.replicator.timer.flow

import kotlinx.coroutines.flow.Flow

interface TimerFactory {

    fun fixedRateFlow(name: String? = null, initialDelay: Long = 0L, period: Long): Flow<TimeTick>

    fun randomRateFlow(name: String? = null, initialDelay: LongRange, period: LongRange): Flow<TimeTick>

    fun expirationFlow(name: String? = null, flow: Flow<*>, period: LongRange): Flow<TimeTick>

    fun sourceMergedExpirationFlow(name: String? = null, flow: Flow<*>, period: LongRange): Flow<TimeTick>
}