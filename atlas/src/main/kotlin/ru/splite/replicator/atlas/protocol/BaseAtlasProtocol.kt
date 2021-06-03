package ru.splite.replicator.atlas.protocol

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import ru.splite.replicator.atlas.AtlasMessage
import ru.splite.replicator.atlas.AtlasProtocolConfig
import ru.splite.replicator.atlas.exception.StaleBallotNumberException
import ru.splite.replicator.atlas.exception.UnexpectedCommandStatusException
import ru.splite.replicator.atlas.executor.CommandExecutor
import ru.splite.replicator.atlas.graph.Dependency
import ru.splite.replicator.atlas.id.Id
import ru.splite.replicator.atlas.id.IdGenerator
import ru.splite.replicator.atlas.protocol.CommandCoordinator.CollectAckDecision
import ru.splite.replicator.atlas.protocol.CommandCoordinator.ConsensusAckDecision
import ru.splite.replicator.atlas.state.*
import ru.splite.replicator.statemachine.ConflictIndex
import ru.splite.replicator.transport.NodeIdentifier
import java.util.concurrent.ConcurrentHashMap

class BaseAtlasProtocol(
    override val config: AtlasProtocolConfig,
    private val idGenerator: IdGenerator<NodeIdentifier>,
    private val conflictIndex: ConflictIndex<Dependency, ByteArray>,
    private val commandExecutor: CommandExecutor,
    private val commandStateStore: CommandStateStore = InMemoryCommandStateStore()
) : AtlasProtocol {

    override val address: NodeIdentifier = config.address

    private val mutexPerCommand = ConcurrentHashMap<Id<NodeIdentifier>, Mutex>()

    private val bufferedCommits = ConcurrentHashMap<Id<NodeIdentifier>, AtlasMessage.MCommit>()

    inner class ManagedCommandCoordinator(override val commandId: Id<NodeIdentifier>) : CommandCoordinator {

        private val accepts = mutableSetOf<NodeIdentifier>()

        private val quorumDependencies = QuorumDependencies()

        private val recoveryAcknowledgments by lazy { mutableMapOf<NodeIdentifier, AtlasMessage.MRecoveryAck>() }

        override suspend fun buildCollect(
            commandBytes: ByteArray,
            fastQuorumNodes: Set<NodeIdentifier>,
            handle: Boolean
        ): AtlasMessage.MCollect {
            if (fastQuorumNodes.size != config.fastQuorumSize) {
                error("Fast quorum must be ${config.fastQuorumSize} size but ${fastQuorumNodes.size} received")
            }

            val command = Command.WithPayload(commandBytes)

            val collectMessage = withLock(commandId) { commandState ->
                commandState.checkStatusIs(CommandStatus.START)
                val dependency = Dependency(commandId)
                val dependencies = conflictIndex.putAndGetConflicts(dependency, commandBytes)
                AtlasMessage.MCollect(commandId, command, fastQuorumNodes, dependencies)
            }

            if (handle) {
                val selfCollectAck = handleCollect(address, collectMessage)
                check(selfCollectAck.isAck)
                val selfCollectAckDecision = handleCollectAck(address, selfCollectAck)
                check(selfCollectAckDecision == CollectAckDecision.NONE)
            }
            return collectMessage
        }

        override suspend fun buildCommit(withPayload: Boolean, handle: Boolean): AtlasMessage.MCommit {
            val commitMessage = withLock(commandId) { commandState ->
                check(commandState.status != CommandStatus.START) {
                    "Commit message cannot be created in START status"
                }
                val newConsensusValue = commandState.consensusValue
                    ?: error("No consensus value for commit")
                val command = when (commandState.command) {
                    is Command.WithPayload -> if (withPayload) commandState.command else Command.Empty
                    is Command.WithNoop -> commandState.command
                    is Command.Empty -> error("Cannot commit Empty command")
                }
                AtlasMessage.MCommit(commandId, newConsensusValue, command)
            }
            if (handle) {
                handleCommit(commitMessage)
            }
            return commitMessage
        }

        override suspend fun buildConsensus(handle: Boolean): AtlasMessage.MConsensus {
            val (consensusMessage, selfConsensusAck) = withLock(commandId) { commandState ->
                check(quorumDependencies.isQuorumCompleted(config.fastQuorumSize)) {
                    "MConsensus message cannot be built because of fast quorum uncompleted"
                }
                val newConsensusValue = AtlasMessage.ConsensusValue(
                    commandState.command.isNoop,
                    quorumDependencies.dependenciesUnion
                )
                val consensusMessage = AtlasMessage.MConsensus(commandId, config.processId, newConsensusValue)
                val selfConsensusAck = handleConsensusWithState(consensusMessage, commandState)
                check(selfConsensusAck.isAck)
                consensusMessage to selfConsensusAck
            }
            if (handle) {
                val selfConsensusAckDecision = handleConsensusAck(address, selfConsensusAck)
                check(selfConsensusAckDecision == ConsensusAckDecision.NONE)
            }
            return consensusMessage
        }

        override suspend fun handleConsensusAck(
            from: NodeIdentifier,
            consensusAck: AtlasMessage.MConsensusAck
        ): ConsensusAckDecision = withLock(consensusAck) { commandState ->
            if (!consensusAck.isAck) {
                return ConsensusAckDecision.NONE
            } else if (commandState.ballot != consensusAck.ballot) {
                throw StaleBallotNumberException(commandState.ballot, consensusAck.ballot)
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
                if (commandState.status != CommandStatus.COLLECT || !collectAck.isAck) {
                    return CollectAckDecision.NONE
                }
                quorumDependencies.addParticipant(from, collectAck.remoteDependencies, config.fastQuorumSize)
                if (quorumDependencies.isQuorumCompleted(config.fastQuorumSize)) {
                    return if (quorumDependencies.checkThresholdUnion(config.f)) {
                        check(!commandState.command.isNoop)
                        val newConsensusValue = AtlasMessage.ConsensusValue(
                            isNoop = false,
                            dependencies = quorumDependencies.dependenciesUnion
                        )
                        commandStateStore.setCommandState(
                            commandId = collectAck.commandId,
                            commandState = commandState.copy(consensusValue = newConsensusValue)
                        )
                        CollectAckDecision.COMMIT
                    } else {
                        CollectAckDecision.CONFLICT
                    }
                }
                return CollectAckDecision.NONE
            }

        override suspend fun buildRecovery(): AtlasMessage.MRecovery {
            val (recoveryMessage, selfRecoveryAck) = withLock(commandId) { commandState ->
                val currentBallot = commandState.ballot
                val newBallot = config.processId + config.n * (currentBallot / config.n + 1)

                val recoveryMessage = AtlasMessage.MRecovery(
                    commandId = commandId,
                    command = commandState.command,
                    ballot = newBallot
                )

                val selfRecoveryAck = handleRecoveryWithState(recoveryMessage, commandState)

                //error if received MCommit from self
                check(selfRecoveryAck is AtlasMessage.MRecoveryAck) {
                    "Recovery from state COMMIT doesn't make sense"
                }
                check(selfRecoveryAck.isAck)
                check(selfRecoveryAck.ballot > commandState.acceptedBallot)
                recoveryMessage to selfRecoveryAck
            }
            handleRecoveryAck(address, selfRecoveryAck)
            return recoveryMessage
        }

        override suspend fun handleRecoveryAck(
            from: NodeIdentifier,
            recoveryAck: AtlasMessage.MRecoveryAck
        ): AtlasMessage.MConsensus? {
            val (consensusMessage, selfConsensusAck) = withLock(recoveryAck) { commandState ->
                if (!recoveryAck.isAck) {
                    return null
                } else if (recoveryAck.ballot != commandState.ballot) {
                    throw StaleBallotNumberException(commandState.ballot, recoveryAck.ballot)
                }

                recoveryAcknowledgments[from] = recoveryAck

                if (recoveryAcknowledgments.size != config.recoveryQuorumSize) {
                    return null
                }

                val consensusMessage = buildConsensusForRecovery(commandState)
                val selfConsensusAck = handleConsensusWithState(consensusMessage, commandState)
                check(selfConsensusAck.isAck)

                consensusMessage to selfConsensusAck
            }
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
                        ballot = commandState.ballot,
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
                        ballot = commandState.ballot,
                        consensusValue = AtlasMessage.ConsensusValue(
                            isNoop = notEmptyQuorumAck.consensusValue.isNoop,
                            dependencies = dependencies
                        )
                    )
                }

            return AtlasMessage.MConsensus(
                commandId = commandId,
                ballot = commandState.ballot,
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

            if (commandState.status != CommandStatus.START) {
                return AtlasMessage.MCollectAck(
                    isAck = false,
                    commandId = message.commandId
                )
            }

            if (!message.quorum.contains(address)) {
                val commandStateAfterUpdate = commandStateStore.setCommandState(
                    commandId = message.commandId,
                    commandState = commandState.copy(
                        status = CommandStatus.PAYLOAD,
                        command = message.command
                    )
                )
                bufferedCommits.remove(message.commandId)?.let { bufferedCommitMessage ->
                    LOGGER.debug("Received payload for buffered commit $bufferedCommitMessage")
                    commitCommand(commandStateAfterUpdate, bufferedCommitMessage)
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
                    message.command.payload
                ).plus(message.remoteDependencies)
            }

            val newConsensusValue = AtlasMessage.ConsensusValue(
                isNoop = false,
                dependencies = dependencies
            )
            commandStateStore.setCommandState(
                commandId = message.commandId,
                commandState = commandState.copy(
                    status = CommandStatus.COLLECT,
                    quorum = message.quorum,
                    command = message.command,
                    consensusValue = newConsensusValue
                )
            )

            return AtlasMessage.MCollectAck(
                isAck = true,
                commandId = message.commandId,
                remoteDependencies = dependencies
            )
        }

    override suspend fun handleConsensus(message: AtlasMessage.MConsensus): AtlasMessage.MConsensusAck =
        withLock(message) { commandState ->
            handleConsensusWithState(message, commandState)
        }

    private suspend fun handleConsensusWithState(
        message: AtlasMessage.MConsensus,
        commandState: CommandState
    ): AtlasMessage.MConsensusAck {
        if (commandState.ballot > message.ballot) {
            return AtlasMessage.MConsensusAck(
                isAck = false,
                commandId = message.commandId,
                ballot = commandState.ballot
            )
        }

        commandStateStore.setCommandState(
            commandId = message.commandId,
            commandState = commandState.copy(
                ballot = message.ballot,
                acceptedBallot = message.ballot,
                consensusValue = message.consensusValue
            )
        )

        return AtlasMessage.MConsensusAck(
            isAck = true,
            commandId = message.commandId,
            ballot = message.ballot
        )
    }

    override suspend fun handleCommit(message: AtlasMessage.MCommit): AtlasMessage.MCommitAck =
        withLock(message) { commandState ->
            if (commandState.status == CommandStatus.START) {
                if (message.command == Command.Empty) {
                    bufferedCommits[message.commandId] = message
                    LOGGER.debug("Commit will be buffered until the payload arrives. commandId=${message.commandId}")
                    return AtlasMessage.MCommitAck(
                        isAck = false,
                        commandId = message.commandId
                    )
                } else {
                    LOGGER.debug("Payload received with commit message. commandId=${message.commandId}")
                    val commandStateAfterUpdate = commandStateStore.setCommandState(
                        commandId = message.commandId,
                        commandState = commandState.copy(
                            status = CommandStatus.PAYLOAD,
                            command = message.command
                        )
                    )
                    return commitCommand(commandStateAfterUpdate, message)
                }
            }

            return commitCommand(commandState, message)
        }

    override suspend fun handleRecovery(message: AtlasMessage.MRecovery): AtlasMessage =
        withLock(message) { commandState ->
            handleRecoveryWithState(message, commandState)
        }

    private suspend fun handleRecoveryWithState(
        message: AtlasMessage.MRecovery,
        commandState: CommandState
    ): AtlasMessage {
        if (commandState.status == CommandStatus.COMMIT) {
            val consensusValue = commandState.consensusValue
                ?: error("Command status COMMIT but consensusValue absent")
            return AtlasMessage.MCommit(
                commandId = message.commandId,
                value = consensusValue,
                command = commandState.command
            )
        }

        if (commandState.ballot >= message.ballot) {
            return AtlasMessage.MRecoveryAck(
                isAck = false,
                commandId = message.commandId,
                consensusValue = commandState.consensusValue ?: AtlasMessage.ConsensusValue(true),
                ballot = commandState.ballot,
                acceptedBallot = commandState.acceptedBallot
            )
        }

        val commandStateAfterUpdate = if (commandState.ballot == 0L && commandState.status == CommandStatus.START) {
            val dependency = Dependency(message.commandId)
            val dependencies = when (message.command) {
                is Command.WithPayload -> conflictIndex.putAndGetConflicts(dependency, message.command.payload)
                is Command.WithNoop -> emptySet()
                else -> error("Unexpected command state for recovery ${message.command}")
            }
            val newConsensusValue = AtlasMessage.ConsensusValue(
                isNoop = message.command.isNoop,
                dependencies = dependencies
            )
            commandStateStore.setCommandState(
                commandId = message.commandId,
                commandState = commandState.copy(
                    status = CommandStatus.RECOVERY,
                    command = message.command,
                    ballot = message.ballot,
                    consensusValue = newConsensusValue
                )
            )
        } else {
            commandStateStore.setCommandState(
                commandId = message.commandId,
                commandState = commandState.copy(
                    status = CommandStatus.RECOVERY,
                    command = if (commandState.command.isNoop) message.command else commandState.command,
                    ballot = message.ballot
                )
            )
        }

        return AtlasMessage.MRecoveryAck(
            isAck = true,
            commandId = message.commandId,
            consensusValue = commandStateAfterUpdate.consensusValue!!,
            quorum = commandStateAfterUpdate.quorum,
            ballot = commandStateAfterUpdate.ballot,
            acceptedBallot = commandStateAfterUpdate.acceptedBallot
        )
    }

    override suspend fun getCommandStatus(commandId: Id<NodeIdentifier>): CommandStatus {
        return this.commandStateStore.getCommandState(commandId)?.status ?: CommandStatus.START
    }

    private suspend fun commitCommand(
        commandState: CommandState,
        message: AtlasMessage.MCommit
    ): AtlasMessage.MCommitAck {
        if (commandState.status == CommandStatus.COMMIT) {
            LOGGER.debug("Commit processed idempotently. commandId=${message.commandId}")
            return AtlasMessage.MCommitAck(
                isAck = true,
                commandId = message.commandId
            )
        }

        val command = if (message.value.isNoop) {
            Command.WithNoop
        } else {
            val commandWithPayload = when (message.command) {
                is Command.Empty -> commandState.command
                else -> message.command
            }
            check(!commandWithPayload.isNoop) {
                "Expected payload for commandId=${message.commandId} but received NOOP"
            }
            commandWithPayload
        }
        check(command !is Command.Empty) {
            "Cannot commit commandId=${message.commandId} because payload is empty"
        }

        LOGGER.debug(
            "Committing commandId={}, isNoop={}, dependencies={}",
            message.commandId,
            message.value.isNoop,
            message.value.dependencies
        )
        commandExecutor.commit(message.commandId, command, message.value.dependencies)

        commandStateStore.setCommandState(
            commandId = message.commandId,
            commandState = commandState.copy(
                status = CommandStatus.COMMIT,
                command = command,
                consensusValue = message.value
            )
        )
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
            val commandState = commandStateStore.getCommandState(commandId) ?: CommandState()
            val result = action(commandState)
            //Состояния COMMIT неизменяемо
            if (commandState.status == CommandStatus.COMMIT) {
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

    private fun CommandState.checkStatusIs(expectedStatus: CommandStatus) {
        if (this.status != expectedStatus) {
            throw UnexpectedCommandStatusException(this.status, expectedStatus)
        }
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(javaClass.enclosingClass)
    }
}