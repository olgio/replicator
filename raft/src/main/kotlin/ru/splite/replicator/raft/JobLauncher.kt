package ru.splite.replicator.raft

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import ru.splite.replicator.raft.protocol.RaftProtocol
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
                    sendAppendEntriesToAll(coroutineScope)
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

    private fun sendAppendEntriesToAll(coroutineScope: CoroutineScope) = coroutineScope.launch {
        if (protocol.isLeader) {
            val result = kotlin.runCatching {
                raftProtocolController.sendAppendEntriesIfLeader()
                val indexWithTerm = raftProtocolController.commitLogEntriesIfLeader()
                if (indexWithTerm != null) {
                    raftProtocolController.sendAppendEntriesIfLeader()
                }
            }
            result.onFailure { e ->
                LOGGER.error("Cannot send appendEntries because of nested exception", e)
            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}