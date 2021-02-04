package ru.splite.replicator.raft.message

interface AppendEntriesMessageReceiver {

    suspend fun handleAppendEntries(request: RaftMessage.AppendEntries): RaftMessage.AppendEntriesResponse
}