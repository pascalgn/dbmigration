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

import com.github.pascalgn.dbmigration.config.Context
import com.github.pascalgn.dbmigration.config.Scripts
import com.github.pascalgn.dbmigration.io.CsvReader
import com.github.pascalgn.dbmigration.sql.DecimalHandler
import com.github.pascalgn.dbmigration.sql.Filter
import com.github.pascalgn.dbmigration.sql.SequenceReset
import com.github.pascalgn.dbmigration.sql.Session
import com.github.pascalgn.dbmigration.sql.Table
import com.github.pascalgn.dbmigration.sql.Utils
import com.github.pascalgn.dbmigration.task.Executor
import com.github.pascalgn.dbmigration.task.ExportTask
import com.github.pascalgn.dbmigration.task.FileImportController
import com.github.pascalgn.dbmigration.task.ImportTask
import org.slf4j.LoggerFactory
import java.io.File
import java.net.URL
import java.net.URLClassLoader
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue

class Migration(val context: Context) : Runnable {
    companion object {
        val logger = LoggerFactory.getLogger(Migration::class.java)!!
    }

    override fun run() {
        val exportDir = File(context.root, "export")
        if (!exportDir.isDirectory && !exportDir.mkdir()) {
            throw IllegalStateException("Not a directory: $exportDir")
        }

        addClasspath()
        loadDrivers()

        if (context.source.skip) {
            logger.info("Export will be skipped")
        } else {
            exportData(exportDir)
        }

        if (context.target.skip) {
            logger.info("Import will be skipped")
        } else {
            Session(context.target.jdbc).use {
                importData(exportDir, it)
            }
        }

        logger.info("Migration finished successfully")
    }

    private fun addClasspath() {
        if (context.classpath.isEmpty()) {
            return
        }
        if (javaClass.classLoader !is URLClassLoader) {
            throw IllegalStateException("Not an instance of URLClassLoader: ${javaClass.classLoader}")
        }
        val addURL = URLClassLoader::class.java.getDeclaredMethod("addURL", URL::class.java)
        addURL.isAccessible = true
        for (classpath in context.classpath) {
            val file = File(classpath)
            if (!file.exists()) {
                throw IllegalArgumentException("File not found: $classpath")
            }
            val url = file.toURI().toURL()
            addURL.invoke(javaClass.classLoader, url)
            logger.info("Added classpath entry: {}", url)
        }
    }

    private fun loadDrivers() {
        for (driver in context.drivers) {
            Class.forName(driver)
            logger.info("Loaded driver: {}", driver)
        }
    }

    private fun exportData(outputDir: File) {
        val source = context.source

        val tables = getTables(source.include)

        if (tables.isEmpty()) {
            throw IllegalStateException("No tables found!")
        }

        val filter = Filter(source.include, source.exclude)
        filter.validate(tables)

        tables.removeIf { filter.excludeTable(it.name) }

        if (tables.isEmpty()) {
            throw IllegalStateException("All tables excluded!")
        }

        logger.info("Exporting {} tables...", tables.size)

        if (source.wait > 0) {
            logger.info("Waiting {} seconds before exporting...", source.wait)
            Thread.sleep(source.wait * 1000L)
        }

        Session(source.jdbc).use { session ->
            val tasks = tables.map {
                ExportTask(outputDir, source.overwrite, filter, it, session, source.retries, source.fetchSize)
            }
            Executor(source.threads).execute(tasks)
        }
    }

    private fun getTables(include: Collection<String> = emptyList()): Queue<Table> {
        logger.info("Reading tables...")
        Session(context.source.jdbc).use { session ->
            val tables = ConcurrentLinkedQueue<Table>()
            session.withConnection { connection ->
                val tableNames = mutableListOf<String>()
                val types = arrayOf("TABLE")
                connection.metaData.getTables(null, session.schema, "%", types).use { rs ->
                    while (rs.next()) {
                        tableNames.add(rs.getString("TABLE_NAME"))
                    }
                }
                tableNames.filter { include.isEmpty() || include.contains(it) }.mapTo(tables) {
                    Table(it, Utils.rowCount(session, connection, it), Utils.getColumns(connection, session.schema, it))
                }
            }
            return tables
        }
    }

    private fun importData(inputDir: File, session: Session) {
        if (context.target.deleteBeforeImport) {
            logger.warn("Deleting existing rows before importing")
        }

        val files = ConcurrentLinkedQueue<File>()
        inputDir.listFiles().filter { it.isFile && it.name.endsWith(".bin") }.forEach { files.add(it) }

        logger.debug("Found {} files", files.size)

        if (files.isEmpty()) {
            logger.warn("No files found!")
            return
        }

        val imported = FileImportController(File(context.root, "imported.lst"))

        imported.imported().forEach { logger.info("Already imported: {}", it) }
        files.removeIf { imported[it] }

        if (files.isEmpty()) {
            logger.info("All files already imported!")
            return
        }

        if (context.target.wait > 0) {
            logger.info("Waiting {} seconds before importing...", context.target.wait)
            Thread.sleep(context.target.wait * 1000L)
        }

        executeScripts(session, context.target.before)

        val tableNames = mutableMapOf<String, String>()
        session.withConnection { connection ->
            connection.metaData.getTables(null, session.schema, "%", null).use { rs ->
                while (rs.next()) {
                    val tableName = rs.getString("TABLE_NAME")
                    tableNames.put(tableName.toUpperCase(), tableName)
                }
            }
        }

        if (tableNames.isEmpty()) {
            throw IllegalStateException("No tables found for schema: ${session.schema}")
        }

        logger.info("Importing {} files...", files.size)

        val rounded = File(context.root, "rounded.csv")
        rounded.bufferedWriter().use { writer ->
            val decimalHandler = DecimalHandler(context.target.roundingRule, writer)
            val tasks = files.map { ImportTask(context, imported, tableNames, it, session, decimalHandler) }
            Executor(context.target.threads).execute(tasks)
        }

        executeScripts(session, context.target.after)

        if (context.target.resetSequences.isNotBlank()) {
            val file = File(context.root, context.target.resetSequences)
            if (!file.isFile) {
                throw IllegalArgumentException("Not a file: $file")
            }
            file.inputStream().use { input ->
                SequenceReset(CsvReader(input), session).run()
            }
        }
    }

    private fun executeScripts(session: Session, scripts: Scripts) {
        for (filename in scripts.files) {
            executeScript(session, filename, scripts.continueOnError)
        }
    }

    private fun executeScript(session: Session, filename: String, continueOnError: Boolean) {
        val file = File(context.root, filename)
        if (!file.isFile) {
            throw IllegalArgumentException("Not a file: $file")
        }
        logger.info("Executing script: {}", file)
        file.readText().split(";").forEach { sql ->
            if (sql.isBlank()) {
                return@forEach
            }
            if (logger.isDebugEnabled) {
                val plain = sql.replace(Regex("\\s+"), " ").trim()
                logger.debug("Executing SQL: {}", plain)
            }
            try {
                session.withConnection { connection ->
                    connection.createStatement().use { statement ->
                        statement.execute(sql)
                    }
                }
            } catch (e: Exception) {
                if (continueOnError) {
                    if (logger.isDebugEnabled) {
                        logger.debug("Error executing script!", e)
                    } else {
                        logger.info("Error executing script: {}", e.message)
                    }
                } else {
                    throw e
                }
            }
        }
    }
}
