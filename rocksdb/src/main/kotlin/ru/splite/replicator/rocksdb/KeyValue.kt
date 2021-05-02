package ru.splite.replicator.rocksdb

data class KeyValue<K, V>(val key: K, val value: V)