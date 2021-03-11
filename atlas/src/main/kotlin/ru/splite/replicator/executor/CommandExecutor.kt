package ru.splite.replicator.executor

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.graph.DependencyGraph
import ru.splite.replicator.id.Id
import ru.splite.replicator.statemachine.StateMachine
import kotlin.coroutines.CoroutineContext

class CommandExecutor(
    private val dependencyGraph: DependencyGraph<Dependency>,
    private val stateMachine: StateMachine<ByteArray, ByteArray>
) {

    private val commandsBuffer = mutableMapOf<Dependency, DeferredCommand>()

    private val committedChannel = Channel<DeferredCommand>(capacity = Channel.UNLIMITED)

    suspend fun awaitCommandResponse(commandId: Id<NodeIdentifier>, command: ByteArray): ByteArray {
        val dependency = Dependency(commandId)
        return commandsBuffer.getOrPut(dependency) {
            DeferredCommand(command, CompletableDeferred())
        }.deferredResponse.await()
    }

    fun commit(commandId: Id<NodeIdentifier>, command: ByteArray, dependencies: Set<Dependency>) {
        //TODO noop
        val dependency = Dependency(commandId)
        val deferredCommand = commandsBuffer.getOrPut(dependency) {
            DeferredCommand(command, CompletableDeferred())
        }
        dependencyGraph.commit(dependency, dependencies)
        committedChannel.offer(deferredCommand)
    }

    fun launchCommandExecutor(coroutineContext: CoroutineContext, coroutineScope: CoroutineScope): Job {
        return coroutineScope.launch(coroutineContext) {
            for (committed in committedChannel) {
                val keysToExecute = dependencyGraph.evaluateKeyToExecute()
                LOGGER.debug("Committed commands for execution on the state machine are found: " +
                        keysToExecute.executable.map { it.dot })
                keysToExecute.executable.forEach {
                    val deferredCommand = commandsBuffer[it]
                        ?: error("Cannot resolve command payload $it from buffer")
                    val response = stateMachine.commit(deferredCommand.command)
                    deferredCommand.deferredResponse.complete(response)
                }
            }
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}