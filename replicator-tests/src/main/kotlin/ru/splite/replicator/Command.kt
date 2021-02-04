package ru.splite.replicator

import kotlinx.serialization.Serializable

@Serializable
data class Command(val value: Long)