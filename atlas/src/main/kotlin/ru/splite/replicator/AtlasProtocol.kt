package ru.splite.replicator

import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.id.Id

interface AtlasProtocol {

    val address: NodeIdentifier

    val config: AtlasProtocolConfig

    fun handleCollect(from: NodeIdentifier, message: AtlasMessage.MCollect): AtlasMessage.MCollectAck

    fun handleConsensus(message: AtlasMessage.MConsensus): AtlasMessage.MConsensusAck

    fun handleCommit(message: AtlasMessage.MCommit): AtlasMessage.MCommitAck

    fun handleRecovery(message: AtlasMessage.MRecovery): AtlasMessage

    fun createCommandCoordinator(): CommandCoordinator

    fun createCommandCoordinator(commandId: Id<NodeIdentifier>): CommandCoordinator
}