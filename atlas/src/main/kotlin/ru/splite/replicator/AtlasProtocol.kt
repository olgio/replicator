package ru.splite.replicator

import ru.splite.replicator.id.Id
import ru.splite.replicator.transport.NodeIdentifier

interface AtlasProtocol {

    val address: NodeIdentifier

    val config: AtlasProtocolConfig

    suspend fun handleCollect(from: NodeIdentifier, message: AtlasMessage.MCollect): AtlasMessage.MCollectAck

    fun handleConsensus(message: AtlasMessage.MConsensus): AtlasMessage.MConsensusAck

    suspend fun handleCommit(message: AtlasMessage.MCommit): AtlasMessage.MCommitAck

    fun handleRecovery(message: AtlasMessage.MRecovery): AtlasMessage

    fun createCommandCoordinator(): CommandCoordinator

    fun createCommandCoordinator(commandId: Id<NodeIdentifier>): CommandCoordinator
}