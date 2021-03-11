package ru.splite.replicator.executor

import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.graph.Dependency
import ru.splite.replicator.id.Id

internal class DeferredCommand(
    val commandId: Id<NodeIdentifier>,
    val command: ByteArray,
    val dependencies: Set<Dependency>
)