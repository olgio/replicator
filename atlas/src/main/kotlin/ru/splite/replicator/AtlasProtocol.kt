package ru.splite.replicator

import ru.splite.replicator.id.Id
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
}