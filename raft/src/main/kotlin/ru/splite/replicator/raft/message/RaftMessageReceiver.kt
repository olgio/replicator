package ru.splite.replicator.raft.message

interface RaftMessageReceiver<C> : VoteRequestMessageReceiver<C>, AppendEntriesMessageReceiver<C>