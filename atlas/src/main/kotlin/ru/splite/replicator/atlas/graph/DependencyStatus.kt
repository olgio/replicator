package ru.splite.replicator.atlas.graph

import kotlinx.serialization.Serializable

@Serializable
enum class DependencyStatus {
    COMMITTED, APPLIED
}