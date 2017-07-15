package com.github.pascalgn.dbmigration

import java.io.File
import java.util.Properties

class Main {
    companion object {
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

            Migration(Context.fromProperties(root, properties)).run()
        }
    }
}
