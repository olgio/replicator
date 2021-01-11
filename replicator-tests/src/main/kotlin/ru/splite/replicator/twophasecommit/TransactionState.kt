package ru.splite.replicator.twophasecommit

import ru.splite.replicator.bus.NodeIdentifier
import java.util.*
import java.util.concurrent.ConcurrentHashMap

data class TransactionState<T>(
    val command: T,
    val ackNodes: MutableSet<NodeIdentifier> = Collections.newSetFromMap(ConcurrentHashMap())
)
