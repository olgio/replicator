package ru.splite.replicator.raft

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filterNotNull
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import ru.splite.replicator.raft.event.IndexWithTerm
import ru.splite.replicator.raft.protocol.RaftProtocol
import ru.splite.replicator.statemachine.StateMachine
import ru.splite.replicator.statemachine.StateMachineCommandSubmitter
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext

/**
 * Реализация отправки команд на репликацию протоколом Raft
 */
class RaftCommandSubmitter(
    private val controller: RaftProtocolController,
    private val stateMachine: StateMachine<ByteArray, ByteArray>
) : StateMachineCommandSubmitter<ByteArray, ByteArray> {

    val protocol: RaftProtocol
        get() = controller.protocol

    private val lastAppliedStateFlow = MutableStateFlow<IndexWithTerm?>(null)

    private val commandResults = ConcurrentHashMap<IndexWithTerm, ByteArray>()

    fun launchCommandApplier(coroutineContext: CoroutineContext, coroutineScope: CoroutineScope): Job {
        return coroutineScope.launch(coroutineContext) {
            protocol.commitEventFlow.collect { lastCommitEvent ->
                var nextIndexToApply: Long = lastAppliedStateFlow.value?.index?.plus(1) ?: 0
                if (lastCommitEvent.index != null) {
                    while (lastCommitEvent.index >= nextIndexToApply) {
                        LOGGER.debug("Detected command to commit index=$nextIndexToApply")
                        val logEntry = protocol.replicatedLogStore.getLogEntryByIndex(nextIndexToApply)
                            ?: error("Index $nextIndexToApply committed but logEntry is null")
                        val result: ByteArray = stateMachine.apply(logEntry.command)
                        val indexWithTerm = IndexWithTerm(index = nextIndexToApply, term = logEntry.term)
                        commandResults[indexWithTerm] = result
                        LOGGER.debug("Applied command to StateMachine $indexWithTerm")
                        lastAppliedStateFlow.tryEmit(indexWithTerm)
                        nextIndexToApply++
                    }
                }
            }
        }
    }

    override suspend fun submit(command: ByteArray): ByteArray {
        val indexWithTerm = if (protocol.isLeader)
            protocol.appendCommand(command) else controller.redirectAndAppendCommand(command)
        LOGGER.debug("Awaiting command result for $indexWithTerm")
        return withTimeout(controller.config.commandExecutorTimeout) {
            val result = awaitCommandResult(indexWithTerm)
            LOGGER.debug("Awaited command result for $indexWithTerm")
            result
        }
    }

    private suspend fun awaitCommandResult(indexWithTerm: IndexWithTerm): ByteArray {
        lastAppliedStateFlow.filterNotNull().first {
            it.index >= indexWithTerm.index
        }
        return commandResults.remove(indexWithTerm)
            ?: error("Cannot extract command result for $indexWithTerm")
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}