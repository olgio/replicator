package ru.splite.replicator.id

import kotlinx.serialization.Serializable

@Serializable
data class Id<S>(val node: S, val id: Long)