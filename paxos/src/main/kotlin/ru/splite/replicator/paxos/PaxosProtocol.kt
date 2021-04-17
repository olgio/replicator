package ru.splite.replicator.paxos

import ru.splite.replicator.paxos.message.PaxosMessageReceiver
import ru.splite.replicator.raft.RaftProtocol

interface PaxosProtocol : RaftProtocol, PaxosMessageReceiver