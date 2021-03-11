package ru.splite.replicator

import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.id.Id

interface AtlasProtocol {

    val nodeIdentifier: NodeIdentifier

    fun createCommandCoordinator(): CommandCoordinator

    fun createCommandCoordinator(commandId: Id<NodeIdentifier>): CommandCoordinator
}