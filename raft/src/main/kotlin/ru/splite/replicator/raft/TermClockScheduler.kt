package ru.splite.replicator.raft

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import ru.splite.replicator.timer.flow.TimerFactory
import kotlin.coroutines.CoroutineContext

class TermClockScheduler(
    private val raftProtocol: RaftProtocol,
    private val timerFactory: TimerFactory
) {

    fun launchTermClock(coroutineContext: CoroutineContext, coroutineScope: CoroutineScope, period: LongRange): Job {
        return coroutineScope.launch(coroutineContext) {
            timerFactory
                .expirationFlow(flow = raftProtocol.leaderAliveFlow, period = period)
                .collect {
                    if (!raftProtocol.isLeader) {
                        raftProtocol.sendVoteRequestsAsCandidate()
                    }
                }
        }
    }
}