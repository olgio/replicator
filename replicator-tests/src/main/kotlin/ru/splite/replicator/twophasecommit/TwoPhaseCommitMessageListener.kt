package ru.splite.replicator.twophasecommit

import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.MessageBus
import ru.splite.replicator.bus.MessageListener
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.statemachine.StateMachine
import java.util.*
import java.util.concurrent.ConcurrentHashMap

class TwoPhaseCommitMessageListener<T>(
    private val nodeIdentifier: NodeIdentifier,
    private val messageBus: MessageBus<TwoPhaseCommitMessage<T>>,
    private val stateMachine: StateMachine<T, Unit>
) : MessageListener<TwoPhaseCommitMessage<T>> {


    private val guidToCommand = ConcurrentHashMap<String, TransactionState<T>>()

    suspend fun applyCommand(command: T) {
        val guid = UUID.randomUUID().toString()
        val message = TwoPhaseCommitMessage.AckRequest(nodeIdentifier, guid, command)
        messageBus.getNodes().forEach { dstNodeIdentifier ->
            messageBus.sendMessage(dstNodeIdentifier, message)
        }
    }

    override suspend fun handleMessage(from: NodeIdentifier, message: TwoPhaseCommitMessage<T>) {
        LOGGER.debug("{} :: Received {}", nodeIdentifier, message)

        when (message) {
            is TwoPhaseCommitMessage.AckRequest -> {
                guidToCommand[message.guid] = TransactionState(message.command)
                messageBus.sendMessage(message.from, TwoPhaseCommitMessage.AckResponse(nodeIdentifier, message.guid))
            }
            is TwoPhaseCommitMessage.AckResponse -> {
                guidToCommand[message.guid]?.let { state ->
                    if (state.ackNodes.add(message.from) && state.ackNodes.size == messageBus.getNodes().size) {
                        LOGGER.debug(
                            "{} :: Detected {}/{} ack",
                            nodeIdentifier,
                            state.ackNodes.size,
                            messageBus.getNodes().size
                        )
                        val commitMessage = TwoPhaseCommitMessage.Commit<T>(nodeIdentifier, message.guid)
                        messageBus.getNodes().forEach { dstNodeIdentifier ->
                            messageBus.sendMessage(dstNodeIdentifier, commitMessage)
                        }
                    }
                }
            }
            is TwoPhaseCommitMessage.RejResponse -> {
                guidToCommand[message.guid]?.let { state ->
                    val rollbackMessage = TwoPhaseCommitMessage.Rollback<T>(nodeIdentifier, message.guid)
                    messageBus.getNodes().forEach { dstNodeIdentifier ->
                        messageBus.sendMessage(dstNodeIdentifier, rollbackMessage)
                    }
                    guidToCommand.remove(message.guid)
                }
            }
            is TwoPhaseCommitMessage.Commit -> {
                guidToCommand[message.guid]?.let { state ->
                    stateMachine.commit(state.command)
                }
            }
            is TwoPhaseCommitMessage.Rollback -> {
                guidToCommand.remove(message.guid)
            }
        }
    }

    companion object {
        val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}