package ru.splite.replicator.executor

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.graph.DependencyGraph
import ru.splite.replicator.id.Id
import ru.splite.replicator.metrics.Metrics
import ru.splite.replicator.metrics.Metrics.measureAndRecord
import ru.splite.replicator.statemachine.StateMachine
import ru.splite.replicator.transport.NodeIdentifier
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext

class CommandExecutor(
    private val dependencyGraph: DependencyGraph<Dependency>,
    private val stateMachine: StateMachine<ByteArray, ByteArray>
) {

    private object NewCommitEvent

    private val commandBlockersChannel = Channel<Id<NodeIdentifier>>(capacity = Channel.UNLIMITED)

    val commandBlockersFlow: Flow<Id<NodeIdentifier>> = commandBlockersChannel.receiveAsFlow()

    private val commandBuffer = ConcurrentHashMap<Id<NodeIdentifier>, ByteArray>()

    private val completableDeferredResponses = ConcurrentHashMap<Id<NodeIdentifier>, CompletableDeferred<ByteArray>>()

    private val committedChannel = Channel<NewCommitEvent>(capacity = Channel.UNLIMITED)

    private val graphMutex = Mutex()

    suspend fun awaitCommandResponse(commandId: Id<NodeIdentifier>, action: suspend () -> Unit): ByteArray {
        try {
            val deferredResponse = completableDeferredResponses.getOrPut(commandId) {
                CompletableDeferred()
            }
            action()
            return deferredResponse.await()
        } finally {
            completableDeferredResponses.remove(commandId)
        }
    }

    suspend fun commit(commandId: Id<NodeIdentifier>, command: ByteArray, dependencies: Set<Dependency>) {
        //TODO noop
        commandBuffer[commandId] = command
        graphMutex.withLock {
            dependencyGraph.commit(Dependency(commandId), dependencies)
        }
        committedChannel.offer(NewCommitEvent)
        LOGGER.debug("Added to graph commandId=$commandId, dependencies=${dependencies.map { it.dot }}")
    }

    fun launchCommandExecutor(coroutineContext: CoroutineContext, coroutineScope: CoroutineScope): Job {
        return coroutineScope.launch(coroutineContext) {
            for (newCommitEvent in committedChannel) {
                Metrics.registry.atlasCommandExecutorLatency.measureAndRecord {
                    executeAvailableCommands()
                }
            }
        }
    }

    private suspend fun executeAvailableCommands() {
        val keysToExecute = graphMutex.withLock {
            dependencyGraph.evaluateKeyToExecute()
        }

        LOGGER.debug(
            "Executing available commands. " +
                    "executable=${keysToExecute.executable.map { it.dot }} " +
                    "blockers=${keysToExecute.blockers.map { it.dot }}"
        )

        keysToExecute.executable.forEach {
            executeCommand(it.dot)
        }

        keysToExecute.blockers.forEach {
            commandBlockersChannel.send(it.dot)
        }
    }

    private fun executeCommand(commandId: Id<NodeIdentifier>) {
        LOGGER.debug("Executing on state machine commandId=$commandId")
        val response = kotlin.runCatching {
            stateMachine.apply(
                commandBuffer.remove(commandId)
                    ?: error("Cannot extract command from buffer. commandId = $commandId")
            )
        }
        LOGGER.debug("Executed on state machine commandId=$commandId")

        completableDeferredResponses[commandId]?.let { deferredResponse ->
            deferredResponse.completeWith(response)
            if (response.isFailure) {
                LOGGER.error(
                    "Cannot execute commandId=$commandId because of nested exception",
                    response.exceptionOrNull()
                )
                response.getOrThrow()
            } else {
                LOGGER.debug("Completed deferred for awaiting client request. commandId=$commandId")
            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}