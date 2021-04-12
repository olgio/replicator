package ru.splite.replicator.cluster

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.demo.keyvalue.KeyValueCommand
import ru.splite.replicator.demo.keyvalue.KeyValueReply
import java.util.*
import kotlin.test.Test


class AtlasClusterTests {

    private val atlasClusterBuilder = AtlasClusterBuilder()

    @Test
    fun successReplicationSingleKeySingleNodeTest(): Unit = runBlockingTest {
        val key = "key"
        atlasClusterBuilder.buildNodes(this, 3, 1) { nodes ->
            repeat(3) {
                nodes[0].submitPutCommand(key, UUID.randomUUID().toString())
            }
            awaitTermination()
            cancelJobs()
            assertNodesHasSameState(nodes)
        }
    }

    @Test
    fun successReplicationSingleKeyMultipleNodesTest(): Unit = runBlockingTest {
        val key = "key"
        atlasClusterBuilder.buildNodes(this, 3, 1) { nodes ->
            nodes.forEach {
                it.submitPutCommand(key, UUID.randomUUID().toString())
            }
            awaitTermination()
            cancelJobs()
            assertNodesHasSameState(nodes)
        }
    }

    @Test
    fun failedNodesReplication3_1Test(): Unit = runBlockingTest {
        val key = "key"
        atlasClusterBuilder.buildNodes(this, 3, 1) { nodes ->
            //success replication if only one one isolated
            listOf(nodes[2]).forEach {
                transport.setNodeIsolated(it.address, true)
            }
            nodes[0].submitPutCommand(key)

            //fail if two nodes isolated
            listOf(nodes[1], nodes[2]).forEach {
                transport.setNodeIsolated(it.address, true)
            }
            val result = kotlin.runCatching {
                nodes[0].submitPutCommand(key)
            }
            assertThat(result.exceptionOrNull()).isNotNull()
        }
    }

    @Test
    fun failedNodesReplication5_2Test(): Unit = runBlockingTest {
        val key = "key"
        atlasClusterBuilder.buildNodes(this, 5, 2) { nodes ->
            //success replication if only two nodes isolated
            listOf(nodes[3], nodes[4]).forEach {
                transport.setNodeIsolated(it.address, true)
            }
            nodes[0].submitPutCommand(key)

            //fail if two nodes isolated
            listOf(nodes[2], nodes[3], nodes[4]).forEach {
                transport.setNodeIsolated(it.address, true)
            }
            val result = kotlin.runCatching {
                nodes[0].submitPutCommand(key)
            }
            assertThat(result.exceptionOrNull()).isNotNull()
        }
    }

    @Test
    fun commandRecoveryAfterFailure5_2Test(): Unit = runBlockingTest {
        val key = "key"
        atlasClusterBuilder.buildNodes(this, 5, 2) { nodes ->

            listOf(nodes[2], nodes[3], nodes[4]).forEach {
                transport.setNodeIsolated(it.address, true)
            }

            val result = kotlin.runCatching {
                nodes[0].submitPutCommand(key)
            }
            assertThat(result.exceptionOrNull())
                .hasStackTraceContaining("Cannot choose decision for recovery")

            listOf(nodes[2], nodes[3], nodes[4]).forEach {
                transport.setNodeIsolated(it.address, false)
            }
            val value = nodes[0].submitPutCommand(key)

            delay(1000)
            assertNodesHasState(nodes, mapOf(key to value))

            nodes.forEach {
                it.submitPutCommand(key)
            }
            awaitTermination()
            cancelJobs()
            assertNodesHasSameState(nodes)
        }
    }

    private suspend fun AtlasClusterNode.submitPutCommand(
        key: String,
        value: String = UUID.randomUUID().toString()
    ): String {
        KeyValueCommand.newPutCommand(key, value).let { command ->
            val commandReply = KeyValueReply.deserializer(this.commandSubmitter.submit(command))
            assertThat(commandReply.value).isEqualTo(value)
        }
        return value
    }

    private fun assertNodesHasState(nodes: List<AtlasClusterNode>, state: Map<String, String>) {
        nodes.forEach {
            assertThat(it.stateMachine.currentState).isEqualTo(state)
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