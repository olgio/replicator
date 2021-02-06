package ru.splite.replicator.raft

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import ru.splite.replicator.timer.flow.TimerFactory

class TermClockScheduler(
    private val raftProtocol: RaftProtocol,
    private val timerFactory: TimerFactory
) {

    fun launchTermClock(coroutineScope: CoroutineScope, period: LongRange): Job {
        val coroutineName = CoroutineName("${raftProtocol.nodeIdentifier}|term-clock")
        return coroutineScope.launch(coroutineName) {
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