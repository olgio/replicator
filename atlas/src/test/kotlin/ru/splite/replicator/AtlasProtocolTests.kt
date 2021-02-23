package ru.splite.replicator

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.AtlasProtocolController.*
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.id.InMemoryIdGenerator
import ru.splite.replicator.keyvalue.KeyValueCommand
import ru.splite.replicator.keyvalue.KeyValueStateMachine
import ru.splite.replicator.transport.CoroutineChannelTransport
import ru.splite.replicator.transport.Transport
import kotlin.test.Test

class AtlasProtocolTests {

    @Test
    fun fastPathQuorumTest(): Unit = runBlockingTest {
        val transport = buildTransport()
        val (node1, node2, node3) = transport.buildNodes(3, 1)

        val command = KeyValueCommand.newPutCommand("1", "value1")

        node1.createCommandCoordinator().let { coordinator ->
            val collectMessage = coordinator.buildCollect(command, setOf(node1.address, node2.address))

            assertThat(collectMessage.quorum).containsExactlyInAnyOrder(node1.address, node2.address)

            val collectAckMessage2 = node1.send(node2.address, collectMessage) as AtlasMessage.MCollectAck
            val collectAckMessage3 = node1.send(node3.address, collectMessage) as AtlasMessage.MCollectAck

            assertThat(collectAckMessage2.isAck).isTrue
            assertThat(collectAckMessage3.isAck).isFalse

            assertThat(coordinator.handleCollectAck(node3.address, collectAckMessage3))
                .isEqualTo(CollectAckDecision.NONE)
            assertThat(coordinator.handleCollectAck(node2.address, collectAckMessage2))
                .isEqualTo(CollectAckDecision.COMMIT)

            val commitMessage = coordinator.buildCommit()

            val commitAckMessage2 = node1.send(node2.address, commitMessage) as AtlasMessage.MCommitAck
            val commitAckMessage3 = node1.send(node3.address, commitMessage) as AtlasMessage.MCommitAck

            assertThat(commitAckMessage2.isAck).isTrue
            assertThat(commitAckMessage3.isAck).isTrue
        }
    }

    @Test
    fun slowPathQuorumTest(): Unit = runBlockingTest {
        val transport = buildTransport()
        val (node1, node2, node3, node4, node5) = transport.buildNodes(5, 2)

        val command1 = KeyValueCommand.newPutCommand("1", "value1")
        val command5 = KeyValueCommand.newPutCommand("1", "value2")

        val fastQuorum1 = setOf(node1, node2, node3, node4).map { it.address }.toSet()
        val fastQuorum2 = setOf(node2, node3, node4, node5).map { it.address }.toSet()

        val coordinator1 = node1.createCommandCoordinator()
        val coordinator5 = node5.createCommandCoordinator()

        val collectMessage1 = coordinator1.buildCollect(command1, fastQuorum1)
        val collectMessage5 = coordinator5.buildCollect(command5, fastQuorum2)

        coordinator1.sendCollectAndAssert(collectMessage1, setOf(node2, node3), CollectAckDecision.NONE)

        coordinator5.sendCollectAndAssert(collectMessage5, setOf(node3, node4), CollectAckDecision.NONE)
        coordinator5.sendCollectAndAssert(collectMessage5, setOf(node2), CollectAckDecision.COMMIT)

        coordinator1.sendCollectAndAssert(collectMessage1, setOf(node4), CollectAckDecision.CONFLICT)

        val consensusMessage1 = coordinator1.buildConsensus()

        coordinator1.sendConsensusAndAssert(consensusMessage1, setOf(node2), ConsensusAckDecision.NONE)
        coordinator1.sendConsensusAndAssert(consensusMessage1, setOf(node3), ConsensusAckDecision.COMMIT)


        val commitMessage5 = coordinator5.buildCommit()
        assertThat(commitMessage5.value.dependencies.map { it.dot })
            .containsExactlyInAnyOrder(coordinator1.commandId)

        val commitMessage1 = coordinator1.buildCommit()
        assertThat(commitMessage1.value.dependencies.map { it.dot })
            .containsExactlyInAnyOrder(coordinator5.commandId)
    }

    @Test
    fun bufferedCommitTest(): Unit = runBlockingTest {
        val transport = buildTransport()
        val (node1, node2, node3) = transport.buildNodes(3, 1)

        val command = KeyValueCommand.newPutCommand("1", "value1")

        node1.createCommandCoordinator().let { coordinator ->
            val collectMessage = coordinator.buildCollect(command, setOf(node1.address, node2.address))


            val commitMessage = coordinator.buildCommit()
            val commitAckMessage = node1.send(node3.address, commitMessage) as AtlasMessage.MCommitAck
            assertThat(commitAckMessage.isAck).isFalse

            val collectAckMessage = node1.send(node3.address, collectMessage) as AtlasMessage.MCollectAck
        }
    }

    private suspend fun CommandCoordinator.sendCollectAndAssert(
        collectMessage: AtlasMessage.MCollect,
        to: Set<AtlasProtocolController>,
        expectDecision: CollectAckDecision
    ) {
        to.forEach { dstNode ->
            val collectAckMessage = parent.send(dstNode.address, collectMessage) as AtlasMessage.MCollectAck
            assertThat(collectAckMessage.isAck).isTrue
            assertThat(this.handleCollectAck(dstNode.address, collectAckMessage))
                .isEqualTo(expectDecision)
        }
    }

    private suspend fun CommandCoordinator.sendConsensusAndAssert(
        collectMessage: AtlasMessage.MConsensus,
        to: Set<AtlasProtocolController>,
        expectDecision: ConsensusAckDecision
    ) {
        to.forEach { dstNode ->
            val consensusAckMessage = parent.send(dstNode.address, collectMessage) as AtlasMessage.MConsensusAck
            assertThat(consensusAckMessage.isAck).isTrue
            assertThat(this.handleConsensusAck(dstNode.address, consensusAckMessage))
                .isEqualTo(expectDecision)
        }
    }

    private fun CoroutineScope.buildTransport(): CoroutineChannelTransport {
        return CoroutineChannelTransport(this)
    }

    private fun Transport.buildNode(i: Int, n: Int, f: Int): AtlasProtocolController {
        val nodeIdentifier = NodeIdentifier("node-$i")
        val stateMachine = KeyValueStateMachine()
        val config = AtlasProtocolConfig(n = n, f = f)
        val idGenerator = InMemoryIdGenerator(nodeIdentifier)
        return AtlasProtocolController(
            nodeIdentifier,
            this,
            i.toLong(),
            idGenerator,
            stateMachine.newConflictIndex(),
            config
        )
    }

    private fun Transport.buildNodes(n: Int, f: Int): List<AtlasProtocolController> {
        return (1..n).map {
            buildNode(it, n, f)
        }
    }
}