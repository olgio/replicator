package ru.splite.replicator.id

interface IdGenerator<S> {

    fun generateNext(): Id<S>
}