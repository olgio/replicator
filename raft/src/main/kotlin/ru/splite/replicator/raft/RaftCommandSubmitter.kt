package ru.splite.replicator.raft

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import ru.splite.replicator.statemachine.StateMachine
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext

class RaftCommandSubmitter(
    val raftProtocol: RaftProtocol,
    private val stateMachine: StateMachine<ByteArray, ByteArray>
) : StateMachineCommandSubmitter<ByteArray, ByteArray> {

    private val lastAppliedStateFlow = MutableStateFlow(-1L)

    private val commandResults = ConcurrentHashMap<Long, ByteArray>()

    fun launchCommandApplier(coroutineContext: CoroutineContext, coroutineScope: CoroutineScope): Job {
        return coroutineScope.launch(coroutineContext) {
            raftProtocol.commitEventFlow.collect { lastCommitEvent ->
                var nextIndexToApply: Long = lastAppliedStateFlow.value + 1
                if (lastCommitEvent.index != null) {
                    while (lastCommitEvent.index >= nextIndexToApply) {
                        val logEntry = raftProtocol.replicatedLogStore.getLogEntryByIndex(nextIndexToApply)
                            ?: error("Index $nextIndexToApply committed but logEntry is null")
                        val result: ByteArray = stateMachine.apply(logEntry.command)
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
        lastAppliedStateFlow.first {
            it >= index
        }
        return commandResults.remove(index)
            ?: error("Cannot extract command result for index $index")
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}