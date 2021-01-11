package ru.splite.replicator.raft.timer

interface RenewableTimerTask {

    fun renew()

    fun cancel()
}