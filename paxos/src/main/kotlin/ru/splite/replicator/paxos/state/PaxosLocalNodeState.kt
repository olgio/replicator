package ru.splite.replicator.paxos.state

import ru.splite.replicator.bus.NodeIdentifier
import ru.splite.replicator.raft.state.RaftLocalNodeState

class PaxosLocalNodeState(nodeIdentifier: NodeIdentifier, val uniqueNodeIdentifier: Long) :
    RaftLocalNodeState(nodeIdentifier)