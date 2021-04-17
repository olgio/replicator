package ru.splite.replicator.raft

fun Int.asMajority(): Int = Math.floorDiv(this, 2) + 1