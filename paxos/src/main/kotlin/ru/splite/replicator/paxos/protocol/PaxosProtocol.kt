package ru.splite.replicator.paxos.protocol

import ru.splite.replicator.paxos.message.PaxosMessageReceiver
import ru.splite.replicator.raft.protocol.RaftProtocol

interface PaxosProtocol : RaftProtocol, PaxosMessageReceiver