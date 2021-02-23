package ru.splite.replicator

import kotlinx.coroutines.flow.*
import org.slf4j.LoggerFactory
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.id.Id
import ru.splite.replicator.id.IdGenerator
import ru.splite.replicator.statemachine.ConflictIndex
import ru.splite.replicator.transport.Transport
import ru.splite.replicator.transport.TypedActor
import ru.splite.replicator.transport.sender.MessageSender

class AtlasProtocolController(
    address: NodeIdentifier,
    transport: Transport,
    private val processId: Long,
    private val idGenerator: IdGenerator<NodeIdentifier>,
    private val conflictIndex: ConflictIndex<Dependency, ByteArray>,
    private val config: AtlasProtocolConfig
) : TypedActor<AtlasMessage>(address, transport, AtlasMessage.serializer()) {

    private val messageSender = MessageSender(this, 1000L)

    private val commands = mutableMapOf<Id<NodeIdentifier>, CommandState>()

    private val bufferedCommits = mutableMapOf<Id<NodeIdentifier>, AtlasMessage.MCommit>()

    enum class CollectAckDecision { COMMIT, CONFLICT, NONE }

    enum class ConsensusAckDecision { COMMIT, NONE }

    inner class CommandCoordinator(val commandId: Id<NodeIdentifier>) {

        val parent: AtlasProtocolController
            get() = this@AtlasProtocolController

        private val commandState = getCommandState(commandId)

        private val accepts = mutableSetOf<NodeIdentifier>()

        private val quorumDependencies by lazy { QuorumDependencies() }

        fun buildCollect(command: ByteArray, fastQuorumNodes: Set<NodeIdentifier>): AtlasMessage.MCollect {
            if (fastQuorumNodes.size != config.fastQuorumSize) {
                error("Fast quorum must be ${config.fastQuorumSize} size but ${fastQuorumNodes.size} received")
            }

            if (commandState.status != CommandState.Status.START) {
                error("Command $commandId already submitted")
            }
            val dependency = Dependency(commandId)
            val deps = conflictIndex.putAndGetConflicts(dependency, command)

            val collect = AtlasMessage.MCollect(commandId, command, fastQuorumNodes, deps)
            val selfCollectAck = handleCollect(address, collect)
            assert(selfCollectAck.isAck)
            val selfCollectAckDecision = handleCollectAck(address, selfCollectAck)
            assert(selfCollectAckDecision == CollectAckDecision.NONE)
            return collect
        }

        fun buildCommit(): AtlasMessage.MCommit {
            val newConsensusValue = commandState.synodState.consensusValue
                ?: error("No consensus value for commit")
            val commitMessage = AtlasMessage.MCommit(commandId, newConsensusValue)
            handleCommit(commitMessage)
            return commitMessage
        }

        fun buildConsensus(): AtlasMessage.MConsensus {
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

        fun handleConsensusAck(from: NodeIdentifier, consensusAck: AtlasMessage.MConsensusAck): ConsensusAckDecision {
            if (!consensusAck.isAck) {
                return ConsensusAckDecision.NONE
            }
            accepts.add(from)
            if (accepts.size == config.slowQuorumSize) {
                return ConsensusAckDecision.COMMIT
            }
            return ConsensusAckDecision.NONE
        }

        fun handleCollectAck(from: NodeIdentifier, collectAck: AtlasMessage.MCollectAck): CollectAckDecision {
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
    }

    inner class CommandRecoveryCoordinator(val commandId: Id<NodeIdentifier>) {

        val parent: AtlasProtocolController
            get() = this@AtlasProtocolController

        private val commandState = getCommandState(commandId)

        private val accepts = mutableSetOf<NodeIdentifier>()

        private val quorumDependencies by lazy { QuorumDependencies() }

        //TODO
    }

    fun createCommandCoordinator(): CommandCoordinator {
        val commandId = idGenerator.generateNext()
        return CommandCoordinator(commandId)
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
            return AtlasMessage.MCommitAck(
                isAck = false,
                commandId = message.commandId
            )
        }

        if (commandState.status == CommandState.Status.COMMIT) {
            return AtlasMessage.MCommitAck(
                isAck = true,
                commandId = message.commandId
            )
        }

        //TODO add to executor
        LOGGER.info("Commit command ${message.commandId}")
        commandState.status = CommandState.Status.COMMIT
        return AtlasMessage.MCommitAck(
            isAck = true,
            commandId = message.commandId
        )
    }

    override suspend fun receive(src: NodeIdentifier, payload: AtlasMessage): AtlasMessage {
        val response = when (payload) {
            is AtlasMessage.MCollect -> handleCollect(src, payload)
            is AtlasMessage.MCommit -> handleCommit(payload)
            is AtlasMessage.MConsensus -> handleConsensus(payload)
            else -> error("")
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