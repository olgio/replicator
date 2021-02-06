package ru.splite.replicator.raft

import kotlinx.coroutines.*
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import org.slf4j.LoggerFactory
import ru.splite.replicator.statemachine.StateMachine
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import ru.splite.replicator.timer.flow.TimerFactory
import java.util.concurrent.ConcurrentHashMap

class RaftCommandSubmitter(
    val raftProtocol: RaftProtocol,
    private val stateMachine: StateMachine<ByteArray, ByteArray>,
    private val timerFactory: TimerFactory
) : StateMachineCommandSubmitter<ByteArray, ByteArray> {

    private val lastAppliedStateFlow = MutableStateFlow(-1L)

    private val appendEntriesFlow = MutableStateFlow(-1L)

    private val commandResults = ConcurrentHashMap<Long, ByteArray>()

    fun launchAppendEntriesSender(coroutineScope: CoroutineScope, period: LongRange): Job {
        val coroutineName = CoroutineName("${raftProtocol.nodeIdentifier}|append-entries-sender")
        return coroutineScope.launch(coroutineName) {
            timerFactory
                .sourceMergedExpirationFlow(flow = appendEntriesFlow, period = period)
                .collect {
                    println(this.isActive)
                    if (raftProtocol.isLeader) {
                        raftProtocol.sendAppendEntriesIfLeader()
                        raftProtocol.commitLogEntriesIfLeader()
                        LOGGER.debug("AppendEntries collected")
                    }
                }
        }
    }

    fun launchCommandApplier(coroutineScope: CoroutineScope): Job {
        val coroutineName = CoroutineName("${raftProtocol.nodeIdentifier}|command-applier")
        return coroutineScope.launch(coroutineName) {
            raftProtocol.lastCommitIndexFlow.collect { lastCommitEvent ->
                var nextIndexToApply: Long = lastAppliedStateFlow.value + 1
                if (lastCommitEvent.lastCommitIndex != null) {
                    while (lastCommitEvent.lastCommitIndex >= nextIndexToApply) {
                        val logEntry = raftProtocol.replicatedLogStore.getLogEntryByIndex(nextIndexToApply)
                            ?: error("Index $nextIndexToApply committed but logEntry is null")
                        val result: ByteArray = stateMachine.commit(logEntry.command)
                        commandResults[nextIndexToApply] = result
                        lastAppliedStateFlow.tryEmit(nextIndexToApply)
                        nextIndexToApply++
                    }
                }
            }
        }
    }

    override suspend fun submit(command: ByteArray): ByteArray {
        val index = raftProtocol.applyCommand(command)
        appendEntriesFlow.value = index
        withTimeout(3000) {
            lastAppliedStateFlow.first {
                it >= index
            }
        }
        return commandResults.remove(index)
            ?: error("Cannot extract command result for index $index")
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}