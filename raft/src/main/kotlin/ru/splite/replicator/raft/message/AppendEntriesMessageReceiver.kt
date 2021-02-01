package ru.splite.replicator.raft.message

interface AppendEntriesMessageReceiver<C> {

    suspend fun handleAppendEntries(request: RaftMessage.AppendEntries<C>): RaftMessage.AppendEntriesResponse
}