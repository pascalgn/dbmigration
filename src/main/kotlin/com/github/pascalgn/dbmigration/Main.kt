package com.github.pascalgn.dbmigration

import java.io.File
import java.util.*

class Main {
    companion object {
        private val DEFAULT_THREADS = "1"
        private val DEFAULT_CONFIGURATION = "/com/github/pascalgn/dbmigration/migration-defaults.properties"

        @JvmStatic
        fun main(args: Array<String>) {
            if (args.size != 1) {
                throw IllegalArgumentException("usage: main <directory>")
            }

            val root = File(args[0])
            if (!root.isDirectory) {
                throw IllegalStateException("Not a directory: $root")
            }

            val configuration = File(root, "migration.properties")
            if (!configuration.exists()) {
                if (configuration.createNewFile()) {
                    configuration.outputStream().use { out ->
                        Main::class.java.getResourceAsStream(DEFAULT_CONFIGURATION).copyTo(out)
                    }
                }
                throw IllegalStateException("No such file, created defaults: $configuration")
            }

            val properties = Properties()
            configuration.reader(Charsets.ISO_8859_1).use { properties.load(it) }

            val classpath = properties.getProperty("classpath", "").split(",").filter { it.isNotBlank() }
            val drivers = properties.getProperty("drivers", "").split(",").filter { it.isNotBlank() }

            val exportThreads = properties.getProperty("export.threads", DEFAULT_THREADS).toInt()
            val exportJdbc = Jdbc(properties.getProperty("export.jdbc.url"),
                properties.getProperty("export.jdbc.username"),
                properties.getProperty("export.jdbc.password"),
                properties.getProperty("export.jdbc.schema"),
                properties.getProperty("export.jdbc.quotes", "true").toBoolean())
            val export = Export(exportThreads, exportJdbc)

            val importThreads = properties.getProperty("import.threads", DEFAULT_THREADS).toInt()
            val deleteBeforeImport = properties.getProperty("import.deleteBeforeImport", "false").toBoolean()
            val importJdbc = Jdbc(properties.getProperty("import.jdbc.url"),
                properties.getProperty("import.jdbc.username"),
                properties.getProperty("import.jdbc.password"),
                properties.getProperty("import.jdbc.schema"),
                properties.getProperty("import.jdbc.quotes", "true").toBoolean())
            val import = Import(importThreads, deleteBeforeImport, importJdbc)

            Migration(Context(root, classpath, drivers, export, import)).run()
        }
    }
}
