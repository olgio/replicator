package ru.splite.replicator.executor

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.graph.DependencyGraph
import ru.splite.replicator.id.Id
import ru.splite.replicator.statemachine.StateMachine
import java.util.concurrent.ConcurrentHashMap
import kotlin.coroutines.CoroutineContext

class CommandExecutor(
    private val dependencyGraph: DependencyGraph<Dependency>,
    private val stateMachine: StateMachine<ByteArray, ByteArray>
) {

    private val commandBuffer = ConcurrentHashMap<Id<NodeIdentifier>, ByteArray>()

    private val completableDeferredResponses = ConcurrentHashMap<Id<NodeIdentifier>, CompletableDeferred<ByteArray>>()

    private val committedChannel = Channel<DeferredCommand>(capacity = Channel.UNLIMITED)

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

    fun commit(commandId: Id<NodeIdentifier>, command: ByteArray, dependencies: Set<Dependency>) {
        //TODO noop
        val deferredCommand = DeferredCommand(commandId, command, dependencies)
        committedChannel.offer(deferredCommand)
    }

    fun launchCommandExecutor(coroutineContext: CoroutineContext, coroutineScope: CoroutineScope): Job {
        return coroutineScope.launch(coroutineContext) {
            for (deferredCommand in committedChannel) {

                commandBuffer[deferredCommand.commandId] = deferredCommand.command

                dependencyGraph.commit(Dependency(deferredCommand.commandId), deferredCommand.dependencies)

                val keysToExecute = dependencyGraph.evaluateKeyToExecute()

                LOGGER.debug("Queued commandId=${deferredCommand.commandId}. " +
                        "executable=${keysToExecute.executable.map { it.dot }} " +
                        "blockers=${keysToExecute.blockers.map { it.dot }}"
                )

                keysToExecute.executable.forEach {
                    LOGGER.debug("Committing commandId=${it.dot}")
                    val response = kotlin.runCatching {
                        stateMachine.commit(
                            commandBuffer.remove(it.dot)
                                ?: error("Cannot extract command from buffer. commandId = ${it.dot}")
                        )
                    }
                    LOGGER.debug("Committed commandId=${it.dot}")

                    completableDeferredResponses[it.dot]?.let { deferredResponse ->
                        deferredResponse.completeWith(response)
                        if (response.isFailure) {
                            LOGGER.error(
                                "Cannot commit commandId=${it.dot} because of nested exception",
                                response.exceptionOrNull()
                            )
                            response.getOrThrow()
                        } else {
                            LOGGER.debug("Completed deferred response for awaiting client request. commandId=${it.dot}")
                        }
                    }
                }


            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}