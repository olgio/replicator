package ru.splite.replicator.cluster

import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.demo.keyvalue.KeyValueCommand
import ru.splite.replicator.demo.keyvalue.KeyValueReply
import kotlin.test.Test


class AtlasClusterTests {

    private val atlasClusterBuilder = AtlasClusterBuilder()

    @Test
    fun successReplicationTest(): Unit = runBlockingTest {
        atlasClusterBuilder.buildNodes(this, 3, 1) { nodes ->
            KeyValueCommand.newPutCommand("1", "v").let { command ->
                val commandReply = KeyValueReply.deserializer(nodes[0].commandSubmitter.submit(command))
                assertThat(commandReply.value).isEqualTo("v")
            }
            KeyValueCommand.newPutCommand("1", "k").let { command ->
                val commandReply = KeyValueReply.deserializer(nodes[0].commandSubmitter.submit(command))
                assertThat(commandReply.value).isEqualTo("k")
            }
            awaitTermination()
            nodes.forEach {
                assertThat(it.stateMachine.currentState["1"]).isEqualTo("k")
            }
        }
    }

    @Test
    fun successReplicationWhenConflictTest(): Unit = runBlockingTest {
        atlasClusterBuilder.buildNodes(this, 3, 1) { nodes ->
            KeyValueCommand.newPutCommand("1", "v").let { command ->
                val commandReply = KeyValueReply.deserializer(nodes[0].commandSubmitter.submit(command))
                assertThat(commandReply.value).isEqualTo("v")
            }
            KeyValueCommand.newPutCommand("1", "k").let { command ->
                val commandReply = KeyValueReply.deserializer(nodes[2].commandSubmitter.submit(command))
                assertThat(commandReply.value).isEqualTo("k")
            }
            awaitTermination()
            nodes.forEach {
                assertThat(it.stateMachine.currentState["1"]).isEqualTo("k")
            }
        }
    }

    @Test
    fun failedLeaderCommandReplicationTest(): Unit = runBlockingTest {
        atlasClusterBuilder.buildNodes(this, 3, 1) { nodes ->
            //success replication if only one one isolated
            listOf(nodes[2]).forEach {
                transport.setNodeIsolated(it.address, true)
            }
            KeyValueCommand.newPutCommand("1", "v").let { command ->
                nodes[0].commandSubmitter.submit(command)
            }

            //fail if two nodes isolated
            listOf(nodes[1], nodes[2]).forEach {
                transport.setNodeIsolated(it.address, true)
            }
            KeyValueCommand.newPutCommand("1", "k").let { command ->
                val result = kotlin.runCatching {
                    nodes[0].commandSubmitter.submit(command)
                }
                assertThat(result.exceptionOrNull()).hasStackTraceContaining("isolated")
            }
        }
    }
}