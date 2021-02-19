package ru.splite.replicator.id

import java.util.concurrent.atomic.AtomicLong

class InMemoryIdGenerator<S>(private val node: S) : IdGenerator<S> {

    private val nextId = AtomicLong(0L)

    override fun generateNext(): Id<S> {
        return Id(node, nextId.getAndIncrement())
    }
}