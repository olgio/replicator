package ru.splite.replicator.protocol

import ru.splite.replicator.AtlasMessage
import ru.splite.replicator.AtlasProtocolConfig
import ru.splite.replicator.id.Id
import ru.splite.replicator.state.CommandState
import ru.splite.replicator.transport.NodeIdentifier

interface AtlasProtocol {

    val address: NodeIdentifier

    val config: AtlasProtocolConfig

    suspend fun handleCollect(from: NodeIdentifier, message: AtlasMessage.MCollect): AtlasMessage.MCollectAck

    suspend fun handleConsensus(message: AtlasMessage.MConsensus): AtlasMessage.MConsensusAck

    suspend fun handleCommit(message: AtlasMessage.MCommit): AtlasMessage.MCommitAck

    suspend fun handleRecovery(message: AtlasMessage.MRecovery): AtlasMessage

    suspend fun createCommandCoordinator(): CommandCoordinator

    suspend fun createCommandCoordinator(commandId: Id<NodeIdentifier>): CommandCoordinator

    fun getCommandStatus(commandId: Id<NodeIdentifier>): CommandState.Status
}