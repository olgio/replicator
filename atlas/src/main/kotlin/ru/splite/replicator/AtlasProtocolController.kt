package ru.splite.replicator

import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory
import ru.splite.replicator.CommandCoordinator.CollectAckDecision
import ru.splite.replicator.CommandCoordinator.ConsensusAckDecision
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.executor.CommandExecutor
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.id.Id
import ru.splite.replicator.id.IdGenerator
import ru.splite.replicator.statemachine.ConflictIndex
import ru.splite.replicator.transport.Transport
import ru.splite.replicator.transport.TypedActor

class AtlasProtocolController(
    address: NodeIdentifier,
    transport: Transport,
    private val processId: Long,
    private val idGenerator: IdGenerator<NodeIdentifier>,
    private val conflictIndex: ConflictIndex<Dependency, ByteArray>,
    private val commandExecutor: CommandExecutor,
    val config: AtlasProtocolConfig
) : AtlasProtocol, TypedActor<AtlasMessage>(address, transport, AtlasMessage.serializer()) {

    override val nodeIdentifier: NodeIdentifier
        get() = address

    private val commands = mutableMapOf<Id<NodeIdentifier>, CommandState>()

    private val bufferedCommits = mutableMapOf<Id<NodeIdentifier>, AtlasMessage.MCommit>()

    inner class ManagedCommandCoordinator(override val commandId: Id<NodeIdentifier>) : CommandCoordinator {

        val parent: AtlasProtocolController
            get() = this@AtlasProtocolController

        private val commandState = getCommandState(commandId)

        private val accepts = mutableSetOf<NodeIdentifier>()

        private val quorumDependencies by lazy { QuorumDependencies() }

        private val recoveryAcks by lazy { mutableMapOf<NodeIdentifier, AtlasMessage.MRecoveryAck>() }

        override fun buildCollect(command: ByteArray, fastQuorumNodes: Set<NodeIdentifier>): AtlasMessage.MCollect {
            if (fastQuorumNodes.size != config.fastQuorumSize) {
                error("Fast quorum must be ${config.fastQuorumSize} size but ${fastQuorumNodes.size} received")
            }

            if (commandState.status != CommandState.Status.START) {
                error("Command $commandId already submitted")
            }
            val dependency = Dependency(commandId)
            val dependencies = conflictIndex.putAndGetConflicts(dependency, command)

            val collect = AtlasMessage.MCollect(commandId, command, fastQuorumNodes, dependencies)
            val selfCollectAck = handleCollect(address, collect)
            check(selfCollectAck.isAck)
            val selfCollectAckDecision = handleCollectAck(address, selfCollectAck)
            check(selfCollectAckDecision == CollectAckDecision.NONE)
            return collect
        }

        override fun buildCommit(): AtlasMessage.MCommit {
            val newConsensusValue = commandState.synodState.consensusValue
                ?: error("No consensus value for commit")
            val commitMessage = AtlasMessage.MCommit(commandId, newConsensusValue)
            handleCommit(commitMessage)
            return commitMessage
        }

        override fun buildConsensus(): AtlasMessage.MConsensus {
            check(quorumDependencies.isQuorumCompleted(config.fastQuorumSize)) {
                "MConsensus message cannot be built because of fast quorum uncompleted"
            }
            val newConsensusValue = AtlasMessage.ConsensusValue(false, quorumDependencies.dependenciesUnion)
            val consensusMessage = AtlasMessage.MConsensus(commandId, processId, newConsensusValue)
            val selfConsensusAck = handleConsensus(consensusMessage)
            check(selfConsensusAck.isAck)
            check(commandState.synodState.ballot > 0)
            val selfConsensusAckDecision = handleConsensusAck(address, selfConsensusAck)
            check(selfConsensusAckDecision == ConsensusAckDecision.NONE)
            return consensusMessage
        }

        override fun handleConsensusAck(
            from: NodeIdentifier,
            consensusAck: AtlasMessage.MConsensusAck
        ): ConsensusAckDecision {
            if (!consensusAck.isAck) {
                return ConsensusAckDecision.NONE
            }
            accepts.add(from)
            if (accepts.size == config.slowQuorumSize) {
                return ConsensusAckDecision.COMMIT
            }
            return ConsensusAckDecision.NONE
        }

        override fun handleCollectAck(from: NodeIdentifier, collectAck: AtlasMessage.MCollectAck): CollectAckDecision {
            if (commandState.status != CommandState.Status.COLLECT || !collectAck.isAck) {
                return CollectAckDecision.NONE
            }
            quorumDependencies.addParticipant(from, collectAck.remoteDependencies, config.fastQuorumSize)
            if (quorumDependencies.isQuorumCompleted(config.fastQuorumSize)) {
                return if (quorumDependencies.checkThresholdUnion(config.f)) {
                    commandState.synodState.consensusValue = AtlasMessage.ConsensusValue(
                        isNoop = false,
                        dependencies = quorumDependencies.dependenciesUnion
                    )
                    CollectAckDecision.COMMIT
                } else {
                    CollectAckDecision.CONFLICT
                }
            }
            return CollectAckDecision.NONE
        }

        override fun buildRecovery(): AtlasMessage.MRecovery {
            val command = commandState.command
                ?: error("Recovery cannot be initiated from node without payload")

            val currentBallot = commandState.synodState.ballot
            val newBallot = processId + config.n * (currentBallot / config.n + 1)

            val recoveryMessage = AtlasMessage.MRecovery(
                commandId = commandId,
                command = command,
                ballot = newBallot
            )

            val selfRecoveryAck = handleRecovery(recoveryMessage)

            //error if received MCommit from self
            check(selfRecoveryAck is AtlasMessage.MRecoveryAck) {
                "Recovery from state ${commandState.status} doesn't make sense"
            }
            check(selfRecoveryAck.isAck)
            check(selfRecoveryAck.ballot > commandState.synodState.acceptedBallot)

            handleRecoveryAck(address, selfRecoveryAck)

            return recoveryMessage
        }

        override fun handleRecoveryAck(
            from: NodeIdentifier,
            recoveryAck: AtlasMessage.MRecoveryAck
        ): AtlasMessage.MConsensus? {
            if (!recoveryAck.isAck || recoveryAck.ballot != commandState.synodState.ballot) {
                return null
            }

            recoveryAcks[from] = recoveryAck

            if (recoveryAcks.size != config.recoveryQuorumSize) {
                return null
            }

            val consensusMessage = buildConsensusForRecovery()

            val selfConsensusAck = handleConsensus(consensusMessage)
            check(selfConsensusAck.isAck)
            check(commandState.synodState.ballot > processId)
            val selfConsensusAckDecision = handleConsensusAck(address, selfConsensusAck)
            check(selfConsensusAckDecision == ConsensusAckDecision.NONE)

            return consensusMessage
        }

        private fun buildConsensusForRecovery(): AtlasMessage.MConsensus {
            recoveryAcks.values
                .filter { it.acceptedBallot > 0 }
                .maxByOrNull { it.acceptedBallot }
                ?.let { maxAcceptedBallotAck ->
                    return AtlasMessage.MConsensus(
                        commandId = commandId,
                        ballot = commandState.synodState.ballot,
                        consensusValue = maxAcceptedBallotAck.consensusValue
                    )
                }

            val initialNodeIdentifier = commandId.node
            val recoveryQuorum = recoveryAcks.map { it.key }
            recoveryAcks.values
                .firstOrNull { it.quorum.isNotEmpty() }
                ?.let { notEmptyQuorumAck ->
                    val notEmptyQuorum = notEmptyQuorumAck.quorum
                    val dependenciesQuorum = if (recoveryQuorum.contains(initialNodeIdentifier))
                        recoveryQuorum else recoveryQuorum.intersect(notEmptyQuorum)
                    val dependencies =
                        dependenciesQuorum.flatMap { recoveryAcks[it]!!.consensusValue.dependencies }.toSet()
                    return AtlasMessage.MConsensus(
                        commandId = commandId,
                        ballot = commandState.synodState.ballot,
                        consensusValue = AtlasMessage.ConsensusValue(
                            isNoop = false,
                            dependencies = dependencies
                        )
                    )
                }

            return AtlasMessage.MConsensus(
                commandId = commandId,
                ballot = commandState.synodState.ballot,
                consensusValue = AtlasMessage.ConsensusValue(isNoop = true)
            )
        }
    }

    override fun createCommandCoordinator(): ManagedCommandCoordinator {
        val commandId = idGenerator.generateNext()
        return ManagedCommandCoordinator(commandId)
    }

    override fun createCommandCoordinator(commandId: Id<NodeIdentifier>): ManagedCommandCoordinator {
        return ManagedCommandCoordinator(commandId)
    }

//    private fun submitCommand(command: ByteArray): AtlasMessage.MCollect {
//        val commandId = idGenerator.generateNext()
//        val dependency = Dependency(commandId)
//        val deps = conflictIndex.putAndGetConflicts(dependency, command)
//        val fastQuorumNodes = messageSender.getNearestNodes(config.fastQuorumSize)
//        return AtlasMessage.MCollect(commandId, command, fastQuorumNodes, deps)
//    }
//
//    suspend fun submit(command: ByteArray) {
//        val collectMessage = submitCommand(command)
//
//        val commandState = getCommandState(collectMessage.commandId)
//
//        handleCollect(this.address, collectMessage)
//        messageSender.sendToQuorum(collectMessage.quorum) {
//            collectMessage
//        }.mapNotNull {
//            val collectAck = it.response as AtlasMessage.MCollectAck
//            if (collectAck.isAck) {
//                it.dst to collectAck
//            } else {
//                null
//            }
//        }.collect { (from, collectAck) ->
////            if (commandState.status != CommandState.Status.COLLECT) {
////                return
////            }
//
//            commandState.quorumDependencies.addParticipant(from, collectAck.remoteDependencies, this.config.fastQuorumSize)
//        }
//
//        if (commandState.quorumDependencies.isQuorumCompleted(this.config.fastQuorumSize)) {
//
//            //TODO merge
//            val newConsensusValue = AtlasMessage.ConsensusValue(false, commandState.quorumDependencies.dependenciesUnion)
//
//            if (commandState.quorumDependencies.checkThresholdUnion(config.f)) {
//                val commitMessage = AtlasMessage.MCommit(commandId, newConsensusValue)
//                handleCommit(commitMessage)
//                messageSender.sendToQuorum(messageSender.getAllNodes()) {
//                    commitMessage
//                }
//            } else {
//                val slowQuorumNodes = messageSender.getNearestNodes(config.slowQuorumSize)
//
//                val consensusMessage = AtlasMessage.MConsensus(commandId, commandState.synodState.ballot, newConsensusValue)
//                val selfConsensusAck = handleConsensus(consensusMessage)
//                assert(selfConsensusAck.isAck)
//
//                val slowQuorumReceived = messageSender.sendToQuorum(slowQuorumNodes) {
//                    consensusMessage
//                }.map { it.response as AtlasMessage.MConsensusAck }.count { it.isAck }
//
//                if (slowQuorumReceived == slowQuorumNodes.size) {
//                    val commitMessage = AtlasMessage.MCommit(commandId, newConsensusValue)
//                    handleCommit(commitMessage)
//                    messageSender.sendToQuorum(messageSender.getAllNodes()) {
//                        commitMessage
//                    }
//                }
//            }
//        }
//    }

    private fun handleCollect(from: NodeIdentifier, message: AtlasMessage.MCollect): AtlasMessage.MCollectAck {
        val commandState = getCommandState(message.commandId)

        if (commandState.status != CommandState.Status.START) {
            return AtlasMessage.MCollectAck(
                isAck = false,
                commandId = message.commandId
            )
        }

        if (!message.quorum.contains(address)) {
            commandState.status = CommandState.Status.PAYLOAD
            commandState.command = message.command

            //TODO check if there's a buffered commit notification and commit if
            bufferedCommits.remove(message.commandId)?.let { bufferedCommit ->
                LOGGER.debug("Received payload for buffered commit $bufferedCommit")
                handleCommit(bufferedCommit)
            }
            return AtlasMessage.MCollectAck(
                isAck = false,
                commandId = message.commandId
            )
        }

        val dependency = Dependency(message.commandId)
        val dependencies = if (from == this.address) message.remoteDependencies else
            conflictIndex.putAndGetConflicts(dependency, message.command, message.remoteDependencies)

        commandState.status = CommandState.Status.COLLECT
        commandState.quorum = message.quorum
        commandState.command = message.command
        commandState.synodState.consensusValue = AtlasMessage.ConsensusValue(
            isNoop = false,
            dependencies = dependencies
        )

        // create and set consensus value
//        let value = ConsensusValue::with(deps.clone());
//        assert!(info.synod.set_if_not_accepted(|| value));

        return AtlasMessage.MCollectAck(
            isAck = true,
            commandId = message.commandId,
            remoteDependencies = dependencies
        )
        //messageSender.sendOrNull(from, collectAckMessage)
    }

    private fun handleConsensus(message: AtlasMessage.MConsensus): AtlasMessage.MConsensusAck {
        val synodState = getCommandState(message.commandId).synodState

        if (synodState.ballot > message.ballot) {
            return AtlasMessage.MConsensusAck(
                isAck = false,
                commandId = message.commandId,
                ballot = 0
            )
        }

        synodState.consensusValue = message.consensusValue
        synodState.ballot = message.ballot
        synodState.acceptedBallot = message.ballot

        return AtlasMessage.MConsensusAck(
            isAck = true,
            commandId = message.commandId,
            ballot = message.ballot
        )
    }

    private fun handleCommit(message: AtlasMessage.MCommit): AtlasMessage.MCommitAck {
        val commandState = getCommandState(message.commandId)

        //TODO buffered commits
        if (commandState.status == CommandState.Status.START) {
            bufferedCommits[message.commandId] = message
            LOGGER.debug("Commit will be buffered until the payload arrives. commandId = ${message.commandId}")
            return AtlasMessage.MCommitAck(
                isAck = false,
                commandId = message.commandId
            )
        }

        if (commandState.status == CommandState.Status.COMMIT) {
            LOGGER.debug("Commit processed idempotently. commandId = ${message.commandId}")
            return AtlasMessage.MCommitAck(
                isAck = true,
                commandId = message.commandId
            )
        }
        val command = commandState.command
            ?: error("Command in status ${commandState.status} but payload is null")

        commandState.synodState.consensusValue = message.value

        //TODO noop
        commandExecutor.commit(message.commandId, command, message.value.dependencies)

        commandState.status = CommandState.Status.COMMIT
        LOGGER.info("Commit command ${message.commandId}")

        return AtlasMessage.MCommitAck(
            isAck = true,
            commandId = message.commandId
        )
    }

    private fun handleRecovery(message: AtlasMessage.MRecovery): AtlasMessage {
        val commandState = getCommandState(message.commandId)

        if (commandState.status == CommandState.Status.COMMIT) {
            val consensusValue = commandState.synodState.consensusValue
                ?: error("Command status COMMIT but consensusValue absent")
            return AtlasMessage.MCommit(
                commandId = message.commandId,
                value = consensusValue
            )
        }

        if (commandState.synodState.ballot >= message.ballot) {
            return AtlasMessage.MRecoveryAck(
                isAck = false,
                commandId = message.commandId,
                consensusValue = commandState.synodState.consensusValue ?: AtlasMessage.ConsensusValue(false),
                ballot = commandState.synodState.ballot,
                acceptedBallot = commandState.synodState.acceptedBallot
            )
        }

        if (commandState.synodState.ballot == 0L && commandState.status == CommandState.Status.START) {
            commandState.command = message.command
            val dependency = Dependency(message.commandId)
            val dependencies = conflictIndex.putAndGetConflicts(dependency, message.command)
            commandState.synodState.consensusValue = AtlasMessage.ConsensusValue(
                isNoop = false,
                dependencies = dependencies
            )
        }

        commandState.synodState.ballot = message.ballot
        commandState.status = CommandState.Status.RECOVERY

        return AtlasMessage.MRecoveryAck(
            isAck = true,
            commandId = message.commandId,
            consensusValue = commandState.synodState.consensusValue!!,
            quorum = commandState.quorum,
            ballot = commandState.synodState.ballot,
            acceptedBallot = commandState.synodState.acceptedBallot
        )
    }

    override suspend fun receive(src: NodeIdentifier, payload: AtlasMessage): AtlasMessage {
        val response = when (payload) {
            is AtlasMessage.MCollect -> handleCollect(src, payload)
            is AtlasMessage.MCommit -> handleCommit(payload)
            is AtlasMessage.MConsensus -> handleConsensus(payload)
            is AtlasMessage.MRecovery -> handleRecovery(payload)
            else -> error("Received unknown type of message: $payload")
        }
        LOGGER.debug("Received $src -> ${this.address}: $payload -> $response")
        return response
    }

    private fun getCommandState(commandId: Id<NodeIdentifier>): CommandState {
        return this.commands.getOrPut(commandId) {
            CommandState()
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}