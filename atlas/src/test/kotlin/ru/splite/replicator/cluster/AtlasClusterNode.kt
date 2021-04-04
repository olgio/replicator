package ru.splite.replicator.cluster

import ru.splite.replicator.AtlasCommandSubmitter
import ru.splite.replicator.keyvalue.KeyValueStateMachine
import ru.splite.replicator.transport.NodeIdentifier

class AtlasClusterNode(
    val address: NodeIdentifier,
    val commandSubmitter: AtlasCommandSubmitter,
    val stateMachine: KeyValueStateMachine
)