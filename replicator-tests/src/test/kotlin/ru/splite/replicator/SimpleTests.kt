package ru.splite.replicator

import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.channels.actor
import kotlinx.coroutines.delay
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.runBlocking
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.coroutines.node
import ru.splite.replicator.coroutines.topology
import ru.splite.replicator.registry.RegistryCommand
import ru.splite.replicator.registry.RegistryStateMachine
import ru.splite.replicator.statemachine.StateMachine
import ru.splite.replicator.twophasecommit.TwoPhaseCommitMessage
import ru.splite.replicator.twophasecommit.TwoPhaseCommitMessageListener
import kotlin.test.Test

class SimpleTests {

    @Test
    fun test() {
        val registryStateMachine = RegistryStateMachine()

        registryStateMachine.commit(RegistryCommand.PutValue(3L))
        registryStateMachine.commit(RegistryCommand.IncValue(1L))
        assertThat(registryStateMachine.getCurrentValue()).isEqualTo(4L)

        registryStateMachine.commit(RegistryCommand.IncValue(-2L))
        assertThat(registryStateMachine.getCurrentValue()).isEqualTo(2L)
    }

    @Test
    fun actorTest() {

        runBlocking {
            val channel1 = actor<RegistryCommand>(CoroutineName("node-1")) {
                val registryStateMachine: StateMachine<RegistryCommand> = RegistryStateMachine()
                for (message in channel) {
                    registryStateMachine.commit(message)
                }
            }

            val channel2 = actor<RegistryCommand>(CoroutineName("node-2")) {
                val registryStateMachine: StateMachine<RegistryCommand> = RegistryStateMachine()
                for (message in channel) {
                    registryStateMachine.commit(message)
                    channel1.send(message)
                }
            }

            channel2.send(RegistryCommand.PutValue(3L))

            channel1.send(RegistryCommand.PutValue(1L))

            channel2.send(RegistryCommand.IncValue(5L))

            delay(200L)

            channel1.close()
            channel2.close()

        }
    }

    @Test
    fun actorTest2() {

        runBlocking {

            topology<RegistryCommand> {
                node("node-1") {
                    val registryStateMachine: StateMachine<RegistryCommand> = RegistryStateMachine()
                    for (message in channel) {
                        registryStateMachine.commit(message)
                    }
                }

                node("node-2") {
                    val registryStateMachine: StateMachine<RegistryCommand> = RegistryStateMachine()
                    for (message in channel) {
                        registryStateMachine.commit(message)
                        sendMessage(NodeIdentifier("node-1"), message)
                    }
                }

                sendMessage(NodeIdentifier("node-2"), RegistryCommand.PutValue(3L))
                sendMessage(NodeIdentifier("node-2"), RegistryCommand.IncValue(5L))
            }
        }
    }

    @Test
    fun twoPhaseCommitTest() {
        runBlocking(newFixedThreadPoolContext(4, "topology")) {
            topology<TwoPhaseCommitMessage<RegistryCommand>> {
                node("node-1") {
                    val registryStateMachine: StateMachine<RegistryCommand> = RegistryStateMachine()
                    val listener = TwoPhaseCommitMessageListener(nodeIdentifier, this, registryStateMachine)
                    for (message in channel) {
                        listener.handleMessage(nodeIdentifier, message)
                    }
                }

                node("node-2") {
                    val registryStateMachine: StateMachine<RegistryCommand> = RegistryStateMachine()
                    val listener = TwoPhaseCommitMessageListener(nodeIdentifier, this, registryStateMachine)
                    listener.applyCommand(RegistryCommand.PutValue(3L))
                    listener.applyCommand(RegistryCommand.IncValue(2L))
                    listener.applyCommand(RegistryCommand.PutValue(6L))

                    for (message in channel) {
                        listener.handleMessage(nodeIdentifier, message)
                    }
                }


            }
        }
    }
}