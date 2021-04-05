package ru.splite.replicator.paxos.state

import ru.splite.replicator.raft.state.RaftLocalNodeState
import ru.splite.replicator.transport.NodeIdentifier

class PaxosLocalNodeState(nodeIdentifier: NodeIdentifier, val uniqueNodeIdentifier: Long) :
    RaftLocalNodeState(nodeIdentifier)