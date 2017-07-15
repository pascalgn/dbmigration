package com.github.pascalgn.dbmigration

import java.io.File
import java.util.Properties

data class Context(val root: File,
                   val classpath: List<String>, val drivers: List<String>,
                   val source: Source, val target: Target) {
    companion object {
        private val DEFAULT_THREADS = "1"

        fun fromProperties(root: File, properties: Properties): Context {
            val classpath = properties.getProperty("classpath", "").split(",").filter { it.isNotBlank() }
            val drivers = properties.getProperty("drivers", "").split(",").filter { it.isNotBlank() }

            val exportThreads = properties.getProperty("source.threads", DEFAULT_THREADS).toInt()
            val exportJdbc = Jdbc(properties.getProperty("source.jdbc.url"),
                properties.getProperty("source.jdbc.username"),
                properties.getProperty("source.jdbc.password"),
                properties.getProperty("source.jdbc.schema"),
                properties.getProperty("source.jdbc.quotes", "true").toBoolean())
            val export = Source(exportThreads, exportJdbc)

            val importThreads = properties.getProperty("target.threads", DEFAULT_THREADS).toInt()
            val deleteBeforeImport = properties.getProperty("target.deleteBeforeImport", "false").toBoolean()
            val before = properties.getProperty("target.before", "").split(",").filter { it.isNotBlank() }
            val after = properties.getProperty("target.after", "").split(",").filter { it.isNotBlank() }
            val importJdbc = Jdbc(properties.getProperty("target.jdbc.url"),
                properties.getProperty("target.jdbc.username"),
                properties.getProperty("target.jdbc.password"),
                properties.getProperty("target.jdbc.schema"),
                properties.getProperty("target.jdbc.quotes", "true").toBoolean())
            val import = Target(importThreads, deleteBeforeImport, before, after, importJdbc)

            return Context(root, classpath, drivers, export, import)
        }
    }
}
