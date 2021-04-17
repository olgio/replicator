package ru.splite.replicator.atlas.cluster

import ru.splite.replicator.atlas.AtlasCommandSubmitter
import ru.splite.replicator.demo.keyvalue.KeyValueStateMachine
import ru.splite.replicator.transport.NodeIdentifier

class AtlasClusterNode(
    val address: NodeIdentifier,
    val commandSubmitter: AtlasCommandSubmitter,
    val stateMachine: KeyValueStateMachine
)