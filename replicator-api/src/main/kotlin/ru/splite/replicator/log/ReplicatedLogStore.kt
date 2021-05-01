package ru.splite.replicator.log

/**
 * Реплицируемый журнал
 */
interface ReplicatedLogStore {

    /**
     * Установка записи [logEntry] в слот с индексом [index]
     * @throws CommittedLogEntryOverrideException если слот [index] уже подтвержден
     * @throws LogGapException если перезапись приводит к пропуску в логе
     */
    suspend fun setLogEntry(index: Long, logEntry: LogEntry)

    /**
     * Добавление записи [logEntry] в новый слот журнала
     * @return индекс добавленной записи
     */
    suspend fun appendLogEntry(logEntry: LogEntry): Long

    /**
     * Обрезка журнала, начиная с [index] включительно
     * @return индекс последней записи после обрезки ([index] - 1 или null если журнал пуст)
     */
    suspend fun prune(index: Long): Long?

    /**
     * Пометка записи с индексом [index] и всех предшествующих записей подтвержденными
     * @throws LogGapException если слот с индексом [index] или предыдущие слоты еще не заполнены
     * @return индекс последнего подтвержденного слота журнала после обновления
     */
    suspend fun commit(index: Long): Long

    /**
     * Пометка записи с индексом [index] и всех предшествующих записей примененными
     * @throws LogGapException если слот с индексом [index] или предыдущие слоты еще не заполнены
     * @return индекс последнего примененного слота журнала после обновления
     */
    suspend fun markApplied(index: Long): Long

    /**
     * @return запись журнала с индексом [index]
     */
    fun getLogEntryByIndex(index: Long): LogEntry?

    /**
     * @return индекс последнего заполненного слота журнала
     */
    fun lastLogIndex(): Long?

    /**
     * @return индекс последнего подтвержденного слота журнала
     */
    fun lastCommitIndex(): Long?

    /**
     * @return индекс последнего примененного слота журнала
     */
    fun lastAppliedIndex(): Long?
}