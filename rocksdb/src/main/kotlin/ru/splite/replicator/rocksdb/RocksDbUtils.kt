package ru.splite.replicator.rocksdb

import com.google.common.primitives.Longs

fun Long.toByteArray(): ByteArray = Longs.toByteArray(this)

fun ByteArray.asLong(): Long = Longs.fromByteArray(this)

fun ByteArray.asString(): String = String(this)