package ru.splite.replicator.atlas.id

interface IdGenerator<S> {

    fun generateNext(): Id<S>
}