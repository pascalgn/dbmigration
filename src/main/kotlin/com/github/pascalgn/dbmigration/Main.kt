package com.github.pascalgn.dbmigration

import java.io.File
import java.util.*

class Main {
    companion object {
        private val DEFAULT_THREADS = "1"

        @JvmStatic
        fun main(args: Array<String>) {
            if (args.size != 1) {
                throw IllegalArgumentException("usage: main <directory>")
            }

            val root = File(args[0])
            if (!root.isDirectory) {
                throw IllegalStateException("Not a directory: $root")
            }

            val properties = Properties()
            File(root, "migration.properties").reader(Charsets.ISO_8859_1).use { properties.load(it) }

            val inputThreads = properties.getProperty("input.threads", DEFAULT_THREADS).toInt()

            val input = Jdbc(properties.getProperty("input.jdbc.url"),
                properties.getProperty("input.jdbc.username"),
                properties.getProperty("input.jdbc.password"),
                properties.getProperty("input.jdbc.schema"))

            Migration(Context(root, inputThreads, input)).run()
        }
    }
}
