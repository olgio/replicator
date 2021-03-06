package ru.splite.replicator.atlas

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.test.runBlockingTest
import org.assertj.core.api.Assertions.assertThat
import ru.splite.replicator.atlas.executor.CommandExecutor
import ru.splite.replicator.atlas.graph.Dependency
import ru.splite.replicator.atlas.graph.JGraphTDependencyGraph
import ru.splite.replicator.atlas.id.InMemoryIdGenerator
import ru.splite.replicator.atlas.protocol.BaseAtlasProtocol
import ru.splite.replicator.atlas.protocol.CommandCoordinator
import ru.splite.replicator.atlas.protocol.CommandCoordinator.CollectAckDecision
import ru.splite.replicator.atlas.protocol.CommandCoordinator.ConsensusAckDecision
import ru.splite.replicator.atlas.state.Command
import ru.splite.replicator.atlas.state.CommandStatus
import ru.splite.replicator.atlas.state.InMemoryCommandStateStore
import ru.splite.replicator.demo.keyvalue.KeyValueCommand
import ru.splite.replicator.demo.keyvalue.KeyValueStateMachine
import ru.splite.replicator.transport.CoroutineChannelTransport
import ru.splite.replicator.transport.NodeIdentifier
import ru.splite.replicator.transport.Transport
import kotlin.test.Test

class AtlasProtocolTests {

    @Test
    fun fastPathQuorumTest(): Unit = runBlockingTest {
        val transport = buildTransport()
        val (node1, node2, node3) = transport.buildNodes(3, 1)

        val command = KeyValueCommand.newPutCommand("1", "value1")

        node1.protocol.createCommandCoordinator().let { coordinator ->
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

        val coordinator1 = node1.protocol.createCommandCoordinator()
        val coordinator5 = node5.protocol.createCommandCoordinator()

        val collectMessage1 = coordinator1.buildCollect(command1, fastQuorum1)
        val collectMessage5 = coordinator5.buildCollect(command5, fastQuorum2)

        coordinator1.sendCollectAndAssert(node1, collectMessage1, setOf(node2, node3), CollectAckDecision.NONE)

        coordinator5.sendCollectAndAssert(node5, collectMessage5, setOf(node3, node4), CollectAckDecision.NONE)
        coordinator5.sendCollectAndAssert(node5, collectMessage5, setOf(node2), CollectAckDecision.COMMIT)

        coordinator1.sendCollectAndAssert(node1, collectMessage1, setOf(node4), CollectAckDecision.CONFLICT)

        val consensusMessage1 = coordinator1.buildConsensus()

        coordinator1.sendConsensusAndAssert(node1, consensusMessage1, setOf(node2), ConsensusAckDecision.NONE)
        coordinator1.sendConsensusAndAssert(node1, consensusMessage1, setOf(node3), ConsensusAckDecision.COMMIT)


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

        node1.protocol.createCommandCoordinator().let { coordinator ->
            val collectMessage = coordinator.buildCollect(command, setOf(node1.address, node2.address))

            val commitMessage = coordinator.buildCommit()
            val commitAckMessage = node1.send(node3.address, commitMessage) as AtlasMessage.MCommitAck
            assertThat(commitAckMessage.isAck).isFalse

            val collectAckMessage = node1.send(node3.address, collectMessage) as AtlasMessage.MCollectAck
            assertThat(collectAckMessage.isAck).isFalse
            assertThat(node3.protocol.getCommandStatus(collectAckMessage.commandId))
                .isEqualTo(CommandStatus.COMMIT)
        }
    }

    @Test
    fun bufferedPayloadTest(): Unit = runBlockingTest {
        val transport = buildTransport()
        val (node1, node2, node3) = transport.buildNodes(3, 1)

        val command = KeyValueCommand.newPutCommand("1", "value1")

        node1.protocol.createCommandCoordinator().let { coordinator ->
            val collectMessage = coordinator.buildCollect(command, setOf(node1.address, node2.address))

            val collectAckMessage = node1.send(node3.address, collectMessage) as AtlasMessage.MCollectAck
            assertThat(collectAckMessage.isAck).isFalse
            assertThat(node3.protocol.getCommandStatus(collectAckMessage.commandId))
                .isEqualTo(CommandStatus.PAYLOAD)

            val commitMessage = coordinator.buildCommit()
            val commitAckMessage = node1.send(node3.address, commitMessage) as AtlasMessage.MCommitAck
            assertThat(commitAckMessage.isAck).isTrue
            assertThat(node3.protocol.getCommandStatus(collectAckMessage.commandId))
                .isEqualTo(CommandStatus.COMMIT)
        }
    }

    @Test
    fun commitWithPayloadTest(): Unit = runBlockingTest {
        val transport = buildTransport()
        val (node1, node2, node3) = transport.buildNodes(3, 1)

        val command = KeyValueCommand.newPutCommand("1", "value1")

        node1.protocol.createCommandCoordinator().let { coordinator ->
            coordinator.buildCollect(command, setOf(node1.address, node2.address))

            val commitMessage = coordinator.buildCommit(withPayload = true)
            val commitAckMessage = node1.send(node3.address, commitMessage) as AtlasMessage.MCommitAck
            assertThat(commitAckMessage.isAck).isTrue
            assertThat(node3.protocol.getCommandStatus(commitAckMessage.commandId))
                .isEqualTo(CommandStatus.COMMIT)
        }
    }

    @Test
    fun recoveryAfterCollectFromNodeInFastQuorumTest(): Unit = runBlockingTest {
        val transport = buildTransport()
        val (node1, node2, node3) = transport.buildNodes(3, 1)

        val command1 = KeyValueCommand.newPutCommand("1", "value1")
        val command2 = KeyValueCommand.newPutCommand("1", "value2")

        // MCollect(Q = [node1, node2], command = [command1])
        // node1 -> node1, node2
        val commitMessage1 = node1.protocol.createCommandCoordinator().let { coordinator ->
            val collectMessage = coordinator.buildCollect(command1, setOf(node1.address, node2.address))
            coordinator.sendCollectAndAssert(node1, collectMessage, setOf(node2), CollectAckDecision.COMMIT)
            val commitMessage = coordinator.buildCommit()
            assertThat(commitMessage.value.dependencies).isEmpty()
            commitMessage
        }
        // MCollect(Q = [node2, node3], command = [command2])
        // node2 -> node2, node3
        val commitMessage2 = node2.protocol.createCommandCoordinator().let { coordinator ->
            val collectMessage = coordinator.buildCollect(command2, setOf(node2.address, node3.address))
            coordinator.sendCollectAndAssert(node2, collectMessage, setOf(node3), CollectAckDecision.COMMIT)
            val commitMessage = coordinator.buildCommit()
            assertThat(commitMessage.value.dependencies).hasSize(1)
            commitMessage
        }
        // MRecovery(command = [command2])
        // node3 -> node1, node3
        node3.protocol.createCommandCoordinator(commitMessage2.commandId).let { coordinator ->
            val recoveryMessage = coordinator.buildRecovery()
            val recoveryAckMessage = node3.send(node1.address, recoveryMessage) as AtlasMessage.MRecoveryAck
            assertThat(recoveryAckMessage.isAck).isTrue
            assertThat(recoveryAckMessage.consensusValue.dependencies).hasSize(1)
            val consensusMessage = coordinator.handleRecoveryAck(node1.address, recoveryAckMessage)
            assertThat(consensusMessage).isNotNull
            assertThat(consensusMessage!!.consensusValue.dependencies).hasSize(1)
            coordinator.sendConsensusAndAssert(node3, consensusMessage, setOf(node1), ConsensusAckDecision.COMMIT)
            val commitMessage = coordinator.buildCommit()
            assertThat(commitMessage.value.dependencies).hasSize(1)
        }
        // MRecovery(command = [command2])
        // node1 -> node1, node3
        node1.protocol.createCommandCoordinator(commitMessage2.commandId).let { coordinator ->
            val recoveryMessage = coordinator.buildRecovery()
            val replayCommitMessage = node1.send(node3.address, recoveryMessage) as AtlasMessage.MCommit
            assertThat(replayCommitMessage.value.dependencies).hasSize(1)
        }
        // MRecovery(command = [command2])
        // node1 -> node1, node2
        node1.protocol.createCommandCoordinator(commitMessage2.commandId).let { coordinator ->
            val recoveryMessage = coordinator.buildRecovery()
            val replayCommitMessage = node1.send(node2.address, recoveryMessage) as AtlasMessage.MCommit
            assertThat(replayCommitMessage.value.dependencies).hasSize(1)
        }
    }

    @Test
    fun recoveryAfterCollectsConflictTest(): Unit = runBlockingTest {
        val transport = buildTransport()
        val (node1, node2, node3, node4, node5) = transport.buildNodes(5, 2)

        val command1 = KeyValueCommand.newPutCommand("1", "value1")
        val command2 = KeyValueCommand.newPutCommand("1", "value2")

        val coordinator1 = node1.protocol.createCommandCoordinator()

        // MCollect(Q = [node1, node2, node3, node4], command = [command1])
        // node1 -> node1, node2, node3
        val collectMessage1 = coordinator1.let { coordinator ->
            val collectMessage = coordinator.buildCollect(
                command1,
                setOf(node1.address, node2.address, node3.address, node4.address)
            )
            assertThat(collectMessage.remoteDependencies).isEmpty()
            coordinator.sendCollectAndAssert(node1, collectMessage, setOf(node2, node3), CollectAckDecision.NONE)
            collectMessage
        }
        // MCollect(Q = [node2, node3, node4, node5], command = [command2])
        // node5 -> node2, node3, node4, node5
        val collectMessage5 = node5.protocol.createCommandCoordinator().let { coordinator ->
            val collectMessage = coordinator.buildCollect(
                command2,
                setOf(node2.address, node3.address, node4.address, node5.address)
            )
            coordinator.sendCollectAndAssert(node5, collectMessage, setOf(node2, node3), CollectAckDecision.NONE)
            coordinator.sendCollectAndAssert(node5, collectMessage, setOf(node4), CollectAckDecision.COMMIT)
            val commitMessage = coordinator.buildCommit()
            assertThat(commitMessage.value.dependencies).containsExactlyInAnyOrder(Dependency(collectMessage1.commandId))
            collectMessage
        }

        // MCollect(Q = [node1, node2, node3, node4], command = [command1])
        // node1 -> node4
        coordinator1.sendCollectAndAssert(node1, collectMessage1, setOf(node4), CollectAckDecision.CONFLICT)

        // MRecovery(command = [command1])
        // node3 -> node1, node3, node5
        node3.protocol.createCommandCoordinator(collectMessage1.commandId).let { coordinator ->
            coordinator.buildRecovery().let { recoveryMessage ->
                val recoveryAckMessage1 = node3.send(node1.address, recoveryMessage) as AtlasMessage.MRecoveryAck
                assertThat(recoveryAckMessage1.isAck).isTrue
                assertThat(recoveryAckMessage1.consensusValue.dependencies).hasSize(0)
                val consensusMessage1 = coordinator.handleRecoveryAck(node1.address, recoveryAckMessage1)
                assertThat(consensusMessage1).isNull()

                val recoveryAckMessage5 = node3.send(node5.address, recoveryMessage) as AtlasMessage.MRecoveryAck
                assertThat(recoveryAckMessage5.isAck).isTrue
                assertThat(recoveryAckMessage5.consensusValue.dependencies).hasSize(1)
                val consensusMessage5 = coordinator.handleRecoveryAck(node5.address, recoveryAckMessage5)
                assertThat(consensusMessage5).isNotNull
                assertThat(consensusMessage5!!.consensusValue.dependencies).hasSize(1)

                // MConsensus(command = [command1])
                // node3 -> node1, node3, node5
                coordinator.sendConsensusAndAssert(node3, consensusMessage5, setOf(node1), ConsensusAckDecision.NONE)
                coordinator.sendConsensusAndAssert(node3, consensusMessage5, setOf(node5), ConsensusAckDecision.COMMIT)
            }

            val commitMessage = coordinator.buildCommit()
            assertThat(commitMessage.value.dependencies).containsExactlyInAnyOrder(Dependency(collectMessage5.commandId))
        }
    }

    @Test
    fun recoveryAfterCollectFromNodeNotInFastQuorumTest(): Unit = runBlockingTest {

        val transport = buildTransport()
        val (node1, node2, node3, node4, node5) = transport.buildNodes(5, 2)

        val command1 = KeyValueCommand.newPutCommand("key", "value1")

        // MCollect(Q = [node1, node2, node3, node4], command = [command1])
        // node1 -> node1, node2
        val collectMessage1 = node1.protocol.createCommandCoordinator().let { coordinator ->
            val collectMessage = coordinator.buildCollect(
                command1,
                setOf(node1.address, node2.address, node3.address, node4.address)
            )
            assertThat(collectMessage.remoteDependencies).isEmpty()
            coordinator.sendCollectAndAssert(node1, collectMessage, setOf(node2), CollectAckDecision.NONE)
            collectMessage
        }

        // MRecovery(command = [command1])
        // node3 -> node3, node4
        node3.protocol.createCommandCoordinator(collectMessage1.commandId).let { coordinator ->
            val recoveryMessage = coordinator.buildRecovery().apply {
                assertThat(command).isEqualTo(Command.WithNoop)
            }
            val recoveryAckMessage4 = (node3.send(node4.address, recoveryMessage) as AtlasMessage.MRecoveryAck).apply {
                assertThat(isAck).isTrue
                assertThat(consensusValue.dependencies).hasSize(0)
            }
            assertThat(coordinator.handleRecoveryAck(node4.address, recoveryAckMessage4)).isNull()

            val recoveryAckMessage5 = (node3.send(node5.address, recoveryMessage) as AtlasMessage.MRecoveryAck).apply {
                assertThat(isAck).isTrue
                assertThat(consensusValue.dependencies).hasSize(0)
            }
            val consensusMessage5 = coordinator.handleRecoveryAck(node5.address, recoveryAckMessage5).apply {
                assertThat(this).isNotNull
            }!!

            // MConsensus(command = [command1])
            // node3 -> node1, node2, node3
            coordinator.sendConsensusAndAssert(node3, consensusMessage5, setOf(node1), ConsensusAckDecision.NONE)
            coordinator.sendConsensusAndAssert(node3, consensusMessage5, setOf(node2), ConsensusAckDecision.COMMIT)

            coordinator.buildCommit(withPayload = true).apply {
                assertThat(command).isEqualTo(Command.WithNoop)
            }
        }
    }

    @Test
    fun recoveryAfterConsensusTest(): Unit = runBlockingTest {
        val transport = buildTransport()
        val (node1, node2, node3, node4, node5) = transport.buildNodes(5, 2)

        val command1 = KeyValueCommand.newPutCommand("key", "value1")
        val command2 = KeyValueCommand.newPutCommand("key", "value2")

        val coordinator1 = node1.protocol.createCommandCoordinator()

        // MCollect(Q = [node1, node2, node3, node4], command = [command1])
        // node1 -> node1, node2, node3
        val collectMessage1 = coordinator1.let { coordinator ->
            val collectMessage = coordinator.buildCollect(
                command1,
                setOf(node1.address, node2.address, node3.address, node4.address)
            )
            assertThat(collectMessage.remoteDependencies).isEmpty()
            coordinator.sendCollectAndAssert(node1, collectMessage, setOf(node2, node3), CollectAckDecision.NONE)
            collectMessage
        }
        // MCollect(Q = [node2, node3, node4, node5], command = [command2])
        // node5 -> node2, node3, node4, node5
        node5.protocol.createCommandCoordinator().let { coordinator ->
            val collectMessage = coordinator.buildCollect(
                command2,
                setOf(node2.address, node3.address, node4.address, node5.address)
            )
            coordinator.sendCollectAndAssert(node5, collectMessage, setOf(node2, node3), CollectAckDecision.NONE)
            coordinator.sendCollectAndAssert(node5, collectMessage, setOf(node4), CollectAckDecision.COMMIT)
            val commitMessage = coordinator.buildCommit()
            assertThat(commitMessage.value.dependencies).containsExactlyInAnyOrder(Dependency(collectMessage1.commandId))
            collectMessage
        }

        // MCollect(Q = [node1, node2, node3, node4], command = [command1])
        // node1 -> node4
        // MConsensus(command = [command1])
        // node1 -> node1, node2, node3
        coordinator1.let { coordinator ->
            coordinator.sendCollectAndAssert(node1, collectMessage1, setOf(node4), CollectAckDecision.CONFLICT)
            val consensusMessage = coordinator.buildConsensus()
            coordinator.sendConsensusAndAssert(node1, consensusMessage, setOf(node2), ConsensusAckDecision.NONE)
            coordinator.sendConsensusAndAssert(node1, consensusMessage, setOf(node3), ConsensusAckDecision.COMMIT)
        }

        // MRecovery(command = [command1])
        // node3 -> node3, node4, node5
        node3.protocol.createCommandCoordinator(collectMessage1.commandId).let { coordinator ->
            val recoveryMessage = coordinator.buildRecovery()
            val recoveryAckMessage4 = (node3.send(node4.address, recoveryMessage) as AtlasMessage.MRecoveryAck).apply {
                assertThat(isAck).isTrue
            }
            assertThat(coordinator.handleRecoveryAck(node4.address, recoveryAckMessage4)).isNull()

            val recoveryAckMessage5 = (node3.send(node5.address, recoveryMessage) as AtlasMessage.MRecoveryAck).apply {
                assertThat(isAck).isTrue
            }
            val consensusMessage5 = coordinator.handleRecoveryAck(node5.address, recoveryAckMessage5).apply {
                assertThat(this).isNotNull
            }!!

            // MConsensus(command = [command1])
            // node3 -> node1, node2, node3
            coordinator.sendConsensusAndAssert(node3, consensusMessage5, setOf(node1), ConsensusAckDecision.NONE)
            coordinator.sendConsensusAndAssert(node3, consensusMessage5, setOf(node2), ConsensusAckDecision.COMMIT)

            coordinator.buildCommit(withPayload = true).apply {
                assertThat(value.dependencies).hasSize(1)
            }
        }
    }

    @Test
    fun strongConnectedDependenciesTest(): Unit = runBlockingTest {
        val transport = buildTransport()
        val (node1, node2, node3) = transport.buildNodes(3, 1)

        val command1 = KeyValueCommand.newPutCommand("1", "value1")
        val command2 = KeyValueCommand.newPutCommand("1", "value2")

        val coordinator1 = node1.protocol.createCommandCoordinator()

        //Collect from node1
        //fastQuorum = [node1, node2]
        //sent to [node1]
        val collectMessage1 = coordinator1.let { coordinator ->
            val collectMessage = coordinator.buildCollect(command1, setOf(node1.address, node2.address))
            assertThat(collectMessage.remoteDependencies).isEmpty()
            collectMessage
        }
        //Collect from node3
        //fastQuorum = [node2, node3]
        //send to [node2, node3]
        val collectMessage3 = node3.protocol.createCommandCoordinator().let { coordinator ->
            val collectMessage = coordinator.buildCollect(command2, setOf(node2.address, node3.address))
            coordinator.sendCollectAndAssert(node3, collectMessage, setOf(node2), CollectAckDecision.COMMIT)
            val commitMessage = coordinator.buildCommit()
            assertThat(commitMessage.value.dependencies).isEmpty()
            collectMessage
        }

        //Collect from node1
        //fastQuorum = [node1, node2]
        //sent to [node2]
        coordinator1.let { coordinator ->
            coordinator.sendCollectAndAssert(node1, collectMessage1, setOf(node2), CollectAckDecision.COMMIT)
            val commitMessage = coordinator.buildCommit()
            assertThat(commitMessage.value.dependencies).containsExactlyInAnyOrder(Dependency(collectMessage3.commandId))
        }
    }

    private suspend fun CommandCoordinator.sendCollectAndAssert(
        parent: AtlasProtocolController,
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
        parent: AtlasProtocolController,
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
        val dependencyGraph = JGraphTDependencyGraph<Dependency>()
        val config = AtlasProtocolConfig(
            address = nodeIdentifier,
            processId = i.toLong(),
            n = n,
            f = f
        )
        val commandStateStore = InMemoryCommandStateStore()
        val commandExecutor = CommandExecutor(
            config, dependencyGraph, stateMachine, commandStateStore
        )
        val idGenerator = InMemoryIdGenerator(nodeIdentifier)
        val atlasProtocol = BaseAtlasProtocol(
            config, idGenerator, stateMachine.newConflictIndex(), commandExecutor, commandStateStore
        )
        return AtlasProtocolController(this, atlasProtocol)
    }

    private fun Transport.buildNodes(n: Int, f: Int): List<AtlasProtocolController> {
        return (1..n).map {
            buildNode(it, n, f)
        }
    }
}