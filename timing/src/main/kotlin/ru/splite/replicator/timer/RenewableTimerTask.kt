package ru.splite.replicator.timer

interface RenewableTimerTask {

    fun renew()

    fun cancel()
}