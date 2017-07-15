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

            val inputThreads = properties.getProperty("input.threads", DEFAULT_THREADS).toInt()

            val input = Jdbc(properties.getProperty("input.jdbc.url"),
                properties.getProperty("input.jdbc.username"),
                properties.getProperty("input.jdbc.password"),
                properties.getProperty("input.jdbc.schema"),
                properties.getProperty("input.jdbc.quotes", "true").toBoolean())

            val outputThreads = properties.getProperty("output.threads", DEFAULT_THREADS).toInt()

            val output = Jdbc(properties.getProperty("output.jdbc.url"),
                properties.getProperty("output.jdbc.username"),
                properties.getProperty("output.jdbc.password"),
                properties.getProperty("output.jdbc.schema"),
                properties.getProperty("input.jdbc.quotes", "true").toBoolean())

            Migration(Context(root, classpath, drivers, inputThreads, input, outputThreads, output)).run()
        }
    }
}
