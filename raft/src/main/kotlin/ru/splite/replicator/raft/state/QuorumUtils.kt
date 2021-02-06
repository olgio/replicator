package ru.splite.replicator.raft.state

fun Int.asMajority(): Int = Math.floorDiv(this, 2) + 1