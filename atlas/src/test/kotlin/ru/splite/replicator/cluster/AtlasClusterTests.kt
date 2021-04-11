package ru.splite.replicator.cluster

import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.demo.keyvalue.KeyValueCommand
import ru.splite.replicator.demo.keyvalue.KeyValueReply
import java.util.*
import kotlin.test.Test


class AtlasClusterTests {

    private val atlasClusterBuilder = AtlasClusterBuilder()

    @Test
    fun successReplicationTest(): Unit = runBlockingTest {
        val key = "key"
        atlasClusterBuilder.buildNodes(this, 3, 1) { nodes ->
            val value1 = UUID.randomUUID().toString()
            KeyValueCommand.newPutCommand(key, value1).let { command ->
                val commandReply = KeyValueReply.deserializer(nodes[0].commandSubmitter.submit(command))
                assertThat(commandReply.value).isEqualTo(value1)
            }
            val value2 = UUID.randomUUID().toString()
            KeyValueCommand.newPutCommand(key, value2).let { command ->
                val commandReply = KeyValueReply.deserializer(nodes[0].commandSubmitter.submit(command))
                assertThat(commandReply.value).isEqualTo(value2)
            }
            awaitTermination()
            assertNodesHasSameState(nodes)
        }
    }

    @Test
    fun successReplicationWhenConflictTest(): Unit = runBlockingTest {
        val key = "key"
        atlasClusterBuilder.buildNodes(this, 3, 1) { nodes ->
            val value1 = UUID.randomUUID().toString()
            KeyValueCommand.newPutCommand(key, value1).let { command ->
                val commandReply = KeyValueReply.deserializer(nodes[0].commandSubmitter.submit(command))
                assertThat(commandReply.value).isEqualTo(value1)
            }
            val value2 = UUID.randomUUID().toString()
            KeyValueCommand.newPutCommand(key, value2).let { command ->
                val commandReply = KeyValueReply.deserializer(nodes[2].commandSubmitter.submit(command))
                assertThat(commandReply.value).isEqualTo(value2)
            }
            awaitTermination()
            assertNodesHasSameState(nodes)
        }
    }

    @Test
    fun failedLeaderCommandReplicationTest(): Unit = runBlockingTest {
        val key = "key"
        atlasClusterBuilder.buildNodes(this, 3, 1) { nodes ->
            //success replication if only one one isolated
            listOf(nodes[2]).forEach {
                transport.setNodeIsolated(it.address, true)
            }
            KeyValueCommand.newPutCommand(key, "v").let { command ->
                nodes[0].commandSubmitter.submit(command)
            }

            //fail if two nodes isolated
            listOf(nodes[1], nodes[2]).forEach {
                transport.setNodeIsolated(it.address, true)
            }
            KeyValueCommand.newPutCommand(key, "k").let { command ->
                val result = kotlin.runCatching {
                    nodes[0].commandSubmitter.submit(command)
                }
                assertThat(result.exceptionOrNull()).hasStackTraceContaining("isolated")
            }
        }
    }

    private fun assertNodesHasSameState(nodes: List<AtlasClusterNode>) {
        val allKeys = nodes.flatMap { it.stateMachine.currentState.keys }.toSet()
        assertThat(allKeys).isNotEmpty
        allKeys.forEach { key ->
            assertThat(nodes.map {
                it.stateMachine.currentState[key]
            }.toSet()).hasSize(1)
        }
    }
}