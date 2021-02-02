package ru.splite.replicator.paxos.message

import ru.splite.replicator.raft.message.AppendEntriesMessageReceiver

interface PaxosMessageReceiver<C> : PaxosVoteRequestMessageReceiver<C>, AppendEntriesMessageReceiver<C> {
}