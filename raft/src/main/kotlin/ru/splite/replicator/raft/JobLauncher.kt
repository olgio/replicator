package ru.splite.replicator.raft

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import ru.splite.replicator.timer.flow.TimerFactory
import kotlin.coroutines.CoroutineContext

class JobLauncher(
    private val raftProtocolController: RaftProtocolController,
    private val timerFactory: TimerFactory
) {

    private val protocol: RaftProtocol
        get() = raftProtocolController.protocol

    fun launchAppendEntriesSender(
        coroutineContext: CoroutineContext,
        coroutineScope: CoroutineScope,
        period: LongRange
    ): Job {
        return coroutineScope.launch(coroutineContext) {
            timerFactory
                .sourceMergedExpirationFlow(flow = protocol.appendEntryEventFlow, period = period)
                .collect {
                    if (protocol.isLeader) {
                        raftProtocolController.sendAppendEntriesIfLeader()
                        raftProtocolController.commitLogEntriesIfLeader()
                    }
                }
        }
    }

    fun launchTermClock(
        coroutineContext: CoroutineContext,
        coroutineScope: CoroutineScope,
        period: LongRange
    ): Job {
        return coroutineScope.launch(coroutineContext) {
            timerFactory
                .expirationFlow(flow = protocol.leaderAliveEventFlow, period = period)
                .collect {
                    if (!protocol.isLeader) {
                        raftProtocolController.sendVoteRequestsAsCandidate()
                    }
                }
        }
    }
}