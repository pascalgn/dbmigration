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
import com.github.pascalgn.dbmigration.io.BinaryReader
import com.github.pascalgn.dbmigration.sql.Exporter
import com.github.pascalgn.dbmigration.sql.JdbcImporter
import com.github.pascalgn.dbmigration.sql.Session
import com.github.pascalgn.dbmigration.sql.SqlServerImporter
import com.github.pascalgn.dbmigration.sql.Table
import com.github.pascalgn.dbmigration.sql.Utils
import org.slf4j.LoggerFactory
import sun.net.www.content.text.plain
import java.io.File
import java.net.URL
import java.net.URLClassLoader
import java.util.Queue
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

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
        val tables = getTables()

        if (tables.isEmpty()) {
            throw IllegalStateException("No tables found!")
        }

        val it = tables.iterator()
        while (it.hasNext()) {
            val table = it.next()
            if (context.source.exclude.contains(table.name)) {
                it.remove()
            }
        }

        if (tables.isEmpty()) {
            throw IllegalStateException("All tables excluded!")
        }

        logger.info("Exporting {} tables...", tables.size)

        execute(context.source.threads) {
            Exporter(outputDir, context.source.jdbc, tables).run()
        }
    }

    private fun getTables(): Queue<Table> {
        logger.info("Reading tables...")
        Session(context.source.jdbc).use { session ->
            val tables = ConcurrentLinkedQueue<Table>()
            session.withConnection { connection ->
                val tableNames = mutableListOf<String>()
                connection.metaData.getTables(null, session.schema, "%", null).use { rs ->
                    while (rs.next()) {
                        tableNames.add(rs.getString("TABLE_NAME"))
                    }
                }
                tableNames.mapTo(tables) { Table(it, Utils.rowCount(session, connection, it)) }
            }
            return tables
        }
    }

    private fun importData(inputDir: File, session: Session) {
        if (context.target.deleteBeforeImport) {
            logger.warn("Deleting existing rows before importing")
        }

        for (before in context.target.before) {
            executeScript(session, before)
        }

        val files = ConcurrentLinkedQueue<File>()
        inputDir.listFiles().filter { it.isFile && it.name.endsWith(".bin") }.forEach { files.add(it) }

        logger.debug("Found {} files", files.size)

        if (files.isEmpty()) {
            logger.warn("No files found!")
            return
        }

        val imported = File(context.root, "imported.txt")
        if (imported.isFile) {
            imported.readLines().forEach { filename ->
                logger.info("Already imported: {}", filename)
                files.remove(File(inputDir, filename))
            }
        }

        val importSuccessful = { file: File ->
            synchronized(imported) {
                imported.appendText(file.name + System.lineSeparator())
            }
        }

        val tableNames = mutableMapOf<String, String>()
        session.withConnection { connection ->
            connection.metaData.getTables(null, session.schema, "%", null).use { rs ->
                while (rs.next()) {
                    val tableName = rs.getString("TABLE_NAME")
                    tableNames.put(tableName.toUpperCase(), tableName)
                }
            }
        }

        if (files.isEmpty()) {
            logger.info("All files already imported!")
            return
        } else {
            if (tableNames.isEmpty()) {
                throw IllegalStateException("No tables found for schema: ${session.schema}")
            }

            logger.info("Importing {} files...", files.size)
            execute(context.target.threads) {
                while (true) {
                    session.withConnection { connection ->
                        val file = files.poll() ?: return@execute

                        BinaryReader(file.inputStream()).use { reader ->
                            val exportTableName = reader.readTableName()
                            val tableName = tableNames.getOrElse(exportTableName.toUpperCase()) {
                                logger.warn("Table {} not found, skipping import!", exportTableName)
                                return@use
                            }

                            try {
                                if (context.target.deleteBeforeImport) {
                                    Utils.deleteRows(session, connection, tableName)
                                } else if (!Utils.isEmpty(session, connection, tableName)) {
                                    logger.warn("Table {} not empty, skipping import!", tableName)
                                    return@use
                                }

                                if (session.isSqlServer()) {
                                    SqlServerImporter(reader, session, tableName).run()
                                } else {
                                    JdbcImporter(reader, session, tableName, context.target.batchSize).run()
                                }
                            } catch (e: InterruptedException) {
                                throw e
                            } catch (e: Exception) {
                                throw IllegalStateException("Error importing $file", e)
                            }

                            importSuccessful(file)
                        }
                    }
                }
            }
        }

        for (after in context.target.after) {
            executeScript(session, after)
        }
    }

    private inline fun execute(threads: Int, crossinline block: () -> Unit) {
        var errors = false
        val executorService = Executors.newFixedThreadPool(threads)
        for (i in 1..threads) {
            executorService.submit {
                try {
                    block()
                } catch (interruptedException: InterruptedException) {
                    logger.debug("Interrupted!")
                } catch (throwable: Throwable) {
                    errors = true
                    logger.error("Error while executing!", throwable)
                    executorService.shutdownNow()
                }
            }
        }
        executorService.shutdown()
        executorService.awaitTermination(14, TimeUnit.DAYS)
        if (errors) {
            throw IllegalStateException("Errors while executing tasks!")
        }
    }

    private fun executeScript(session: Session, filename: String) {
        val file = File(context.root, filename)
        if (!file.isFile) {
            throw IllegalArgumentException("Not a file: $file")
        }
        logger.info("Executing script: {}", file)
        session.withConnection { connection ->
            connection.createStatement().use { statement ->
                file.readText().split(";").forEach { sql ->
                    if (sql.isBlank()) {
                        return@forEach
                    }
                    if (logger.isDebugEnabled) {
                        val plain = sql.replace(Regex("\\s+"), " ").trim()
                        logger.debug("Executing SQL: {}", plain)
                    }
                    statement.execute(sql)
                }
            }
        }
    }
}
