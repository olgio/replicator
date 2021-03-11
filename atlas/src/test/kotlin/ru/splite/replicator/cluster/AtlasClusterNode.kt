package ru.splite.replicator.cluster

import ru.splite.replicator.AtlasCommandSubmitter
import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.keyvalue.KeyValueStateMachine

class AtlasClusterNode(
    val address: NodeIdentifier,
    val commandSubmitter: AtlasCommandSubmitter,
    val stateMachine: KeyValueStateMachine
)