package ru.splite.replicator.protocol

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import ru.splite.replicator.AtlasMessage
import ru.splite.replicator.AtlasProtocolConfig
import ru.splite.replicator.exception.StaleBallotNumberException
import ru.splite.replicator.exception.UnexpectedCommandStatusException
import ru.splite.replicator.executor.CommandExecutor
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.id.Id
import ru.splite.replicator.id.IdGenerator
import ru.splite.replicator.protocol.CommandCoordinator.CollectAckDecision
import ru.splite.replicator.protocol.CommandCoordinator.ConsensusAckDecision
import ru.splite.replicator.state.Command
import ru.splite.replicator.state.CommandState
import ru.splite.replicator.state.QuorumDependencies
import ru.splite.replicator.statemachine.ConflictIndex
import ru.splite.replicator.transport.NodeIdentifier
import java.util.concurrent.ConcurrentHashMap

class BaseAtlasProtocol(
    override val config: AtlasProtocolConfig,
    private val idGenerator: IdGenerator<NodeIdentifier>,
    private val conflictIndex: ConflictIndex<Dependency, ByteArray>,
    private val commandExecutor: CommandExecutor
) : AtlasProtocol {

    override val address: NodeIdentifier = config.address

    private val commands = ConcurrentHashMap<Id<NodeIdentifier>, CommandState>()

    private val mutexPerCommand = ConcurrentHashMap<Id<NodeIdentifier>, Mutex>()

    private val bufferedCommits = ConcurrentHashMap<Id<NodeIdentifier>, AtlasMessage.MCommit>()

    inner class ManagedCommandCoordinator(override val commandId: Id<NodeIdentifier>) : CommandCoordinator {

        private val accepts = mutableSetOf<NodeIdentifier>()

        private val quorumDependencies by lazy { QuorumDependencies() }

        private val recoveryAcknowledgments by lazy { mutableMapOf<NodeIdentifier, AtlasMessage.MRecoveryAck>() }

        override suspend fun buildCollect(
            commandBytes: ByteArray,
            fastQuorumNodes: Set<NodeIdentifier>
        ): AtlasMessage.MCollect {
            if (fastQuorumNodes.size != config.fastQuorumSize) {
                error("Fast quorum must be ${config.fastQuorumSize} size but ${fastQuorumNodes.size} received")
            }

            val command = Command.WithPayload(commandBytes)

            val collectMessage = withLock(commandId) { commandState ->
                commandState.checkStatusIs(CommandState.Status.START)
                val dependency = Dependency(commandId)
                val dependencies = conflictIndex.putAndGetConflicts(dependency, commandBytes)
                AtlasMessage.MCollect(commandId, command, fastQuorumNodes, dependencies)
            }

            val selfCollectAck = handleCollect(address, collectMessage)
            check(selfCollectAck.isAck)
            val selfCollectAckDecision = handleCollectAck(address, selfCollectAck)
            check(selfCollectAckDecision == CollectAckDecision.NONE)
            return collectMessage
        }

        override suspend fun buildCommit(withPayload: Boolean): AtlasMessage.MCommit {
            val commitMessage = withLock(commandId) { commandState ->
                check(commandState.status != CommandState.Status.START) {
                    "Commit message cannot be created in START status"
                }
                val newConsensusValue = commandState.synodState.consensusValue
                    ?: error("No consensus value for commit")
                val command = when (commandState.command) {
                    is Command.WithPayload -> if (withPayload) commandState.command else Command.Empty
                    is Command.WithNoop -> commandState.command
                    is Command.Empty -> error("Cannot commit Empty command")
                }
                AtlasMessage.MCommit(commandId, newConsensusValue, command)
            }
            handleCommit(commitMessage)
            return commitMessage
        }

        override suspend fun buildConsensus(): AtlasMessage.MConsensus {
            val consensusMessage = withLock(commandId) {
                check(quorumDependencies.isQuorumCompleted(config.fastQuorumSize)) {
                    "MConsensus message cannot be built because of fast quorum uncompleted"
                }
                val newConsensusValue = AtlasMessage.ConsensusValue(false, quorumDependencies.dependenciesUnion)
                AtlasMessage.MConsensus(commandId, config.processId, newConsensusValue)
            }
            val selfConsensusAck = handleConsensus(consensusMessage)
            check(selfConsensusAck.isAck)
//            check(commandState.synodState.ballot > 0)
            val selfConsensusAckDecision = handleConsensusAck(address, selfConsensusAck)
            check(selfConsensusAckDecision == ConsensusAckDecision.NONE)
            return consensusMessage
        }

        override suspend fun handleConsensusAck(
            from: NodeIdentifier,
            consensusAck: AtlasMessage.MConsensusAck
        ): ConsensusAckDecision = withLock(consensusAck) { commandState ->
            if (!consensusAck.isAck) {
                return ConsensusAckDecision.NONE
            } else if (commandState.synodState.ballot != consensusAck.ballot) {
                throw StaleBallotNumberException(commandState.synodState.ballot, consensusAck.ballot)
            }
            accepts.add(from)
            if (accepts.size == config.slowQuorumSize) {
                return ConsensusAckDecision.COMMIT
            }
            return ConsensusAckDecision.NONE
        }

        override suspend fun handleCollectAck(
            from: NodeIdentifier,
            collectAck: AtlasMessage.MCollectAck
        ): CollectAckDecision =
            withLock(collectAck) { commandState ->
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

        override suspend fun buildRecovery(): AtlasMessage.MRecovery {
            val recoveryMessage = withLock(commandId) { commandState ->
                val command = commandState.command

                val currentBallot = commandState.synodState.ballot
                val newBallot = config.processId + config.n * (currentBallot / config.n + 1)

                AtlasMessage.MRecovery(
                    commandId = commandId,
                    command = command,
                    ballot = newBallot
                )
            }

            val selfRecoveryAck = handleRecovery(recoveryMessage)

            //error if received MCommit from self
            check(selfRecoveryAck is AtlasMessage.MRecoveryAck) {
                "Recovery from state COMMIT doesn't make sense"
            }
            check(selfRecoveryAck.isAck)
            //check(selfRecoveryAck.ballot > commandState.synodState.acceptedBallot)

            handleRecoveryAck(address, selfRecoveryAck)

            return recoveryMessage
        }

        override suspend fun handleRecoveryAck(
            from: NodeIdentifier,
            recoveryAck: AtlasMessage.MRecoveryAck
        ): AtlasMessage.MConsensus? {
            val consensusMessage = withLock(recoveryAck) { commandState ->
                if (!recoveryAck.isAck) {
                    return null
                } else if (recoveryAck.ballot != commandState.synodState.ballot) {
                    throw StaleBallotNumberException(commandState.synodState.ballot, recoveryAck.ballot)
                }

                recoveryAcknowledgments[from] = recoveryAck

                if (recoveryAcknowledgments.size != config.recoveryQuorumSize) {
                    return null
                }

                buildConsensusForRecovery(commandState)
            }

            val selfConsensusAck = handleConsensus(consensusMessage)
            check(selfConsensusAck.isAck)
            //check(commandState.synodState.ballot > config.processId)
            val selfConsensusAckDecision = handleConsensusAck(address, selfConsensusAck)
            check(selfConsensusAckDecision == ConsensusAckDecision.NONE)

            return consensusMessage
        }

        private fun buildConsensusForRecovery(commandState: CommandState): AtlasMessage.MConsensus {
            recoveryAcknowledgments.values
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
            val recoveryQuorum = recoveryAcknowledgments.map { it.key }
            recoveryAcknowledgments.values
                .firstOrNull { it.quorum.isNotEmpty() }
                ?.let { notEmptyQuorumAck ->
                    val notEmptyQuorum = notEmptyQuorumAck.quorum
                    val dependenciesQuorum = if (recoveryQuorum.contains(initialNodeIdentifier))
                        recoveryQuorum else recoveryQuorum.intersect(notEmptyQuorum)
                    val dependencies =
                        dependenciesQuorum.flatMap { recoveryAcknowledgments[it]!!.consensusValue.dependencies }.toSet()
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

    override suspend fun createCommandCoordinator(): ManagedCommandCoordinator {
        val commandId = idGenerator.generateNext()
        return ManagedCommandCoordinator(commandId)
    }

    override suspend fun createCommandCoordinator(commandId: Id<NodeIdentifier>): ManagedCommandCoordinator {
        return ManagedCommandCoordinator(commandId)
    }

    override suspend fun handleCollect(from: NodeIdentifier, message: AtlasMessage.MCollect): AtlasMessage.MCollectAck =
        withLock(message) { commandState ->

            if (commandState.status != CommandState.Status.START) {
                return AtlasMessage.MCollectAck(
                    isAck = false,
                    commandId = message.commandId
                )
            }

            if (!message.quorum.contains(address)) {
                commandState.status = CommandState.Status.PAYLOAD
                commandState.command = message.command

                bufferedCommits.remove(message.commandId)?.let { bufferedCommitMessage ->
                    LOGGER.debug("Received payload for buffered commit $bufferedCommitMessage")
                    commitCommand(commandState, bufferedCommitMessage)
                }
                return AtlasMessage.MCollectAck(
                    isAck = false,
                    commandId = message.commandId
                )
            }

            val dependency = Dependency(message.commandId)
            val dependencies = when (from) {
                this.address -> message.remoteDependencies
                else -> conflictIndex.putAndGetConflicts(
                    dependency,
                    message.command.payload,
                    message.remoteDependencies
                )
            }

            commandState.status = CommandState.Status.COLLECT
            commandState.quorum = message.quorum
            commandState.command = message.command
            commandState.synodState.consensusValue = AtlasMessage.ConsensusValue(
                isNoop = false,
                dependencies = dependencies
            )

            return AtlasMessage.MCollectAck(
                isAck = true,
                commandId = message.commandId,
                remoteDependencies = dependencies
            )
        }

    override suspend fun handleConsensus(message: AtlasMessage.MConsensus): AtlasMessage.MConsensusAck =
        withLock(message) { commandState ->
            if (commandState.synodState.ballot > message.ballot) {
                return AtlasMessage.MConsensusAck(
                    isAck = false,
                    commandId = message.commandId,
                    ballot = commandState.synodState.ballot
                )
            }

            commandState.synodState.consensusValue = message.consensusValue
            commandState.synodState.ballot = message.ballot
            commandState.synodState.acceptedBallot = message.ballot

            return AtlasMessage.MConsensusAck(
                isAck = true,
                commandId = message.commandId,
                ballot = message.ballot
            )
        }

    override suspend fun handleCommit(message: AtlasMessage.MCommit): AtlasMessage.MCommitAck =
        withLock(message) { commandState ->
            if (commandState.status == CommandState.Status.START) {
                if (message.command == Command.Empty) {
                    bufferedCommits[message.commandId] = message
                    LOGGER.debug("Commit will be buffered until the payload arrives. commandId = ${message.commandId}")
                    return AtlasMessage.MCommitAck(
                        isAck = false,
                        commandId = message.commandId
                    )
                } else {
                    LOGGER.debug("Payload received with commit message. commandId = ${message.commandId}")
                    commandState.command = message.command
                    commandState.status = CommandState.Status.PAYLOAD
                }
            }

            return commitCommand(commandState, message)
        }

    override suspend fun handleRecovery(message: AtlasMessage.MRecovery): AtlasMessage =
        withLock(message) { commandState ->
            if (commandState.status == CommandState.Status.COMMIT) {
                val consensusValue = commandState.synodState.consensusValue
                    ?: error("Command status COMMIT but consensusValue absent")
                return AtlasMessage.MCommit(
                    commandId = message.commandId,
                    value = consensusValue,
                    command = commandState.command
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
                val dependencies = when (message.command) {
                    is Command.WithPayload -> conflictIndex.putAndGetConflicts(dependency, message.command.payload)
                    else -> emptySet()
                }
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

    override fun getCommandStatus(commandId: Id<NodeIdentifier>): CommandState.Status {
        return this.commands[commandId]?.status ?: CommandState.Status.START
    }

    private suspend fun commitCommand(
        commandState: CommandState,
        message: AtlasMessage.MCommit
    ): AtlasMessage.MCommitAck {
        if (commandState.status == CommandState.Status.COMMIT) {
            LOGGER.debug("Commit processed idempotently. commandId = ${message.commandId}")
            return AtlasMessage.MCommitAck(
                isAck = true,
                commandId = message.commandId
            )
        }
        val command = when (message.command) {
            is Command.Empty -> commandState.command
            else -> message.command
        }
        check(command !is Command.Empty)

        commandState.synodState.consensusValue = message.value

        commandExecutor.commit(message.commandId, command, message.value.dependencies)

        commandState.status = CommandState.Status.COMMIT
        LOGGER.debug("Successfully committed commandId=${message.commandId}")

        return AtlasMessage.MCommitAck(
            isAck = true,
            commandId = message.commandId
        )
    }

    private suspend inline fun <T> withLock(commandId: Id<NodeIdentifier>, action: (CommandState) -> T): T {
        val mutex = this.mutexPerCommand.getOrPut(commandId) {
            Mutex()
        }
        var mutexCanBeRemovedAfterRelease = false
        return mutex.withLock {
            val commandState = this.commands.getOrPut(commandId) {
                CommandState()
            }
            val result = action(commandState)
            //Состояния COMMIT неизменяемо
            if (commandState.status == CommandState.Status.COMMIT) {
                mutexCanBeRemovedAfterRelease = true
            }
            result
        }.also {
            if (mutexCanBeRemovedAfterRelease) {
                this.mutexPerCommand.remove(commandId)
            }
        }
    }

    private suspend inline fun <T> withLock(message: AtlasMessage.PerCommandMessage, action: (CommandState) -> T): T {
        return withLock(message.commandId, action)
    }

    private fun CommandState.checkStatusIs(expectedStatus: CommandState.Status) {
        if (this.status != expectedStatus) {
            throw UnexpectedCommandStatusException(this.status, expectedStatus)
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}