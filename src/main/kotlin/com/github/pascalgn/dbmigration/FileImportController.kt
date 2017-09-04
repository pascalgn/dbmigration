package com.github.pascalgn.dbmigration

import java.io.File

internal class FileImportController(private val file: File) : ImportController, AutoCloseable {
    private val imported = LinkedHashSet<String>()

    init {
        if (file.isFile) {
            imported.addAll(file.readLines())
        }
    }

    fun imported(): Set<String> = imported

    @Synchronized
    override fun get(key: File): Boolean = imported.contains(key.name)

    @Synchronized
    override fun set(key: File, value: Boolean) {
        if (value) {
            imported.add(key.name)
        } else {
            imported.remove(key.name)
        }
    }

    @Synchronized
    override fun close() {
        file.bufferedWriter().use { writer ->
            imported.forEach {
                writer.write(it)
                writer.newLine()
            }
        }
    }
}
