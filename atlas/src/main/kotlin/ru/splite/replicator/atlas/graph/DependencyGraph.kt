package ru.splite.replicator.atlas.graph

/**
 * Граф зависимостей
 * @param K тип вершины в графе
 */
interface DependencyGraph<K : Comparable<K>> {

    /**
     * Число вершин в графе
     */
    val numVertices: Int

    /**
     * Добавление вершины [key] в граф и ее зависимостей (ребер графа) [dependencies]
     */
    suspend fun commit(key: K, dependencies: Set<K>)

    /**
     * Удаление и возвращение ключей, готовых к исполнению
     * @return ключи команд, готовых к исполнению
     */
    suspend fun evaluateKeyToExecute(): KeysToExecute<K>
}