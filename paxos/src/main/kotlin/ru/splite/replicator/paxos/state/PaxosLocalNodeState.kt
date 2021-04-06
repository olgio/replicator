package ru.splite.replicator.paxos.state

import ru.splite.replicator.raft.state.RaftLocalNodeState

class PaxosLocalNodeState(val uniqueNodeIdentifier: Long) : RaftLocalNodeState()