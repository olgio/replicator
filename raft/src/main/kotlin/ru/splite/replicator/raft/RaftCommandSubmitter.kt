package ru.splite.replicator.raft

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import ru.splite.replicator.raft.event.IndexWithTerm
import ru.splite.replicator.statemachine.StateMachine
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext

class RaftCommandSubmitter(
    private val controller: RaftProtocolController,
    private val stateMachine: StateMachine<ByteArray, ByteArray>
) : StateMachineCommandSubmitter<ByteArray, ByteArray> {

    val protocol: RaftProtocol
        get() = controller.protocol

    private val lastAppliedStateFlow = MutableStateFlow(-1L)

    private val commandResults = ConcurrentHashMap<Long, ByteArray>()

    fun launchCommandApplier(coroutineContext: CoroutineContext, coroutineScope: CoroutineScope): Job {
        return coroutineScope.launch(coroutineContext) {
            protocol.commitEventFlow.collect { lastCommitEvent ->
                var nextIndexToApply: Long = lastAppliedStateFlow.value + 1
                if (lastCommitEvent.index != null) {
                    while (lastCommitEvent.index >= nextIndexToApply) {
                        val logEntry = protocol.replicatedLogStore.getLogEntryByIndex(nextIndexToApply)
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
        val indexWithTerm = if (protocol.isLeader)
            protocol.appendCommand(command) else controller.redirectAndAppendCommand(command)
        return awaitCommandResult(indexWithTerm)
    }

    private suspend fun awaitCommandResult(indexWithTerm: IndexWithTerm): ByteArray {
        lastAppliedStateFlow.first {
            it >= indexWithTerm.index
        }
        return commandResults.remove(indexWithTerm.index)
            ?: error("Cannot extract command result for $indexWithTerm")
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}