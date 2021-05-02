package ru.splite.replicator.atlas.rocksdb

import kotlinx.serialization.Serializable

@Serializable
enum class DependencyStatus {
    COMMITTED, APPLIED
}