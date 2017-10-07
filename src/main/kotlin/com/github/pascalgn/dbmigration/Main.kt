/*
 * Copyright 2017 Pascal
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.pascalgn.dbmigration

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.FileAppender
import com.github.pascalgn.dbmigration.config.Context
import org.slf4j.LoggerFactory
import java.io.File
import java.util.Properties
import java.util.regex.Pattern

object Main {
    private val DEFAULT_CONFIGURATION = "/com/github/pascalgn/dbmigration/migration-defaults.properties"

    private val logger = LoggerFactory.getLogger(Main::class.java)!!

    @JvmStatic
    fun main(args: Array<String>) {
        val status = run(args.asList())
        System.exit(status)
    }

    private fun run(args: List<String>): Int {
        if (args.isEmpty() || args[0] == "-h" || args[0] == "--help") {
            return usage()
        }

        val command = args[0]
        return when (command) {
            "migrate" -> migrate(args.drop(1))
            "validate" -> validate(args.drop(1))
            "export" -> export(args.drop(1))
            else -> usage()
        }
    }

    private fun usage(): Int {
        println("usage: dbmigration [-h] <command> ...")
        println()
        println("optional arguments:")
        println("  -h, --help  show this help message and exit")
        println()
        println("commands:")
        println("  migrate     start the database migration")
        println("  validate    run validation on the input files")
        println("  export      export the given input files")
        return 1
    }

    private fun migrate(args: List<String>): Int {
        if (args.size != 1 || args[0] == "-h" || args[0] == "--help") {
            println("usage: dbmigration migrate [-h] <directory>")
            println()
            println("arguments:")
            println("  <directory>  the root directory for the migration")
            return 1
        }

        val root = File(args[0])
        if (!root.isDirectory) {
            throw IllegalStateException("Not a directory: $root")
        }

        try {
            initLogging(root)
        } catch (e: RuntimeException) {
            logger.warn("Could not initialize log file!", e)
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

        return 0
    }

    private fun initLogging(root: File) {
        val file = nextFile(root)
        logger.info("Logging to {}", file.absolutePath)

        val context = LoggerFactory.getILoggerFactory() as LoggerContext

        val encoder = PatternLayoutEncoder()
        encoder.pattern = "%d{yyyy-MM-dd'T'HH:mm:ss.SSSZ} [%thread] %level %logger{1} - %msg%n"
        encoder.context = context
        encoder.start()

        val fileAppender = FileAppender<ILoggingEvent>()
        fileAppender.file = file.absolutePath
        fileAppender.encoder = encoder
        fileAppender.context = context
        fileAppender.start()

        val rootLogger = LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME) as Logger
        rootLogger.addAppender(fileAppender)
    }

    private fun nextFile(dir: File): File {
        val files = dir.list() ?: emptyArray()
        val pattern = Pattern.compile("dbmigration-([0-9]+).log")
        var index = files.map { pattern.matcher(it) }
            .filter { it.matches() }
            .map {
                try {
                    it.group(1).toInt()
                } catch (e: NumberFormatException) {
                    logger.debug("Unexpected file: {}", it.group(), e)
                    return@map 0
                }
            }.max() ?: 0

        // choose the next free file, ignoring gaps, using a positive index
        index += 1

        var file: File
        while (true) {
            file = File(dir, "dbmigration-$index.log")
            if (file.createNewFile()) {
                return file
            } else if (index < Int.MAX_VALUE) {
                index++
            } else {
                throw IllegalStateException("Could not find next log file in $dir")
            }
        }
    }

    private fun validate(args: List<String>): Int {
        if (args.isEmpty() || "-h" in args || "--help" in args) {
            println("usage: dbmigration validate [-h] <file>...")
            println()
            println("arguments:")
            println("  <file>  the files to validate")
            return 1
        }

        Validation(args.map { File(it) }).run()

        return 0
    }

    private fun export(args: List<String>): Int {
        if (args.isEmpty() || "-h" in args || "--help" in args) {
            println("usage: dbmigration export [-h] <file>...")
            println()
            println("arguments:")
            println("  <file>  the files to export")
            return 1
        }

        Export(args.map { File(it) }).run()

        return 0
    }
}
