package ru.splite.replicator.raft

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import ru.splite.replicator.raft.timer.RenewableTimerFactory
import ru.splite.replicator.raft.timer.RenewableTimerTask
import java.util.*
import kotlin.concurrent.fixedRateTimer

class RaftProtocolScheduler(private val raftProtocol: RaftProtocol) {

    private val renewableTimerFactory = RenewableTimerFactory()

    private lateinit var termIncrementerTimerTask: RenewableTimerTask

    private lateinit var appendEntriesSenderTask: Timer

    fun CoroutineScope.scheduleTermClock(delay: Long) {
        termIncrementerTimerTask = renewableTimerFactory.scheduleWithPeriodAndInitialDelay(delay) {
            launch {
                if (!raftProtocol.isLeader) {
                    raftProtocol.sendVoteRequestsAsCandidate()
                }
            }
        }
    }

    fun CoroutineScope.scheduleAppendEntriesSender(delay: Long) {
        appendEntriesSenderTask = fixedRateTimer(initialDelay = delay, period = delay) {
            launch {
                raftProtocol.sendAppendEntriesIfLeader()
                raftProtocol.commitLogEntriesIfLeader()
            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}