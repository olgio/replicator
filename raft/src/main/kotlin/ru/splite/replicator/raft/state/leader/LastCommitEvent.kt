package ru.splite.replicator.raft.state.leader

data class LastCommitEvent(val lastCommitIndex: Long?)
