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

import org.slf4j.LoggerFactory
import java.io.File
import java.io.InputStream
import java.sql.Connection
import java.sql.DriverManager
import java.util.Queue

internal class Importer(val jdbc: Jdbc, val batchSize: Int,
                        val deleteBeforeImport: Boolean, val files: Queue<File>) : Runnable {
    companion object {
        val logger = LoggerFactory.getLogger(Importer::class.java)!!
    }

    override fun run() {
        DriverManager.getConnection(jdbc.url, jdbc.username, jdbc.password).use { connection ->
            if (!connection.autoCommit) {
                connection.autoCommit = true
            }

            val tableNames = mutableMapOf<String, String>()
            connection.metaData.getTables(null, jdbc.schema, "%", null).use { rs ->
                while (rs.next()) {
                    val tableName = rs.getString("TABLE_NAME")
                    tableNames.put(tableName.toUpperCase(), tableName)
                }
            }

            if (tableNames.isEmpty()) {
                throw IllegalStateException("No tables found for schema: ${jdbc.schema}")
            }

            while (true) {
                val file = files.poll() ?: break
                importFile(connection, tableNames, file)
            }
        }
    }

    private fun importFile(connection: Connection, tableNames: Map<String, String>, file: File) {
        file.inputStream().use { input ->
            val reader = Reader(input)

            val exportTableName = reader.readTableName()
            val tableName = tableNames.getOrElse(exportTableName.toUpperCase()) {
                logger.warn("Table {} not found, skipping import!", exportTableName)
                return
            }

            if (deleteBeforeImport) {
                deleteRows(connection, tableName)
            } else if (!isEmpty(connection, tableName)) {
                logger.warn("Table {} not empty, skipping import!", tableName)
                return
            }

            logger.info("Importing {}", tableName)

            val sourceColumns = reader.readColumns()
            val targetColumns = getColumns(connection, tableName)

            val mapping = mutableMapOf<Int, Int>()
            for ((targetIdx, targetColumn) in targetColumns) {
                for ((sourceIdx, sourceColumn) in sourceColumns) {
                    if (sourceColumn.name.toUpperCase() == targetColumn.name.toUpperCase()) {
                        mapping.put(sourceIdx, targetIdx)
                        break
                    }
                }
                if (!mapping.containsValue(targetIdx)) {
                    throw IllegalStateException("Missing source column for target $targetColumn")
                }
            }

            val str = StringBuilder()
            str.append("INSERT INTO ")
            str.append(jdbc.tableName(tableName))
            str.append(" (")
            for (column in targetColumns.values) {
                if (!str.endsWith("(")) {
                    str.append(",")
                }
                str.append(column.name)
            }
            str.append(") VALUES (")
            str.append("?")
            str.append(",?".repeat(targetColumns.size - 1))
            str.append(")")

            logger.debug("Prepared statement: $str")

            connection.prepareStatement(str.toString()).use { statement ->
                var added = 0
                while (reader.nextRow()) {
                    reader.readRow { sourceIdx, value ->
                        val targetIdx = mapping.getOrDefault(sourceIdx, -1)
                        if (targetIdx != -1) {
                            if (value is InputStream) {
                                statement.setBlob(targetIdx, value)
                            } else {
                                statement.setObject(targetIdx, value)
                            }
                        }
                    }
                    if (batchSize == 0) {
                        statement.executeUpdate()
                        statement.clearParameters()
                        ++added
                    } else {
                        statement.addBatch()
                        ++added
                        if (added >= batchSize) {
                            logger.debug("Executing batch: {} rows [{}]", added, tableName)
                            statement.executeBatch()
                            statement.clearBatch()
                            logger.debug("Batch executed [{}]", tableName)
                            added = 0
                        }
                    }
                    if (added % 100 == 0 && Thread.interrupted()) {
                        Thread.currentThread().interrupt()
                        throw InterruptedException()
                    }
                }
                if (batchSize != 0 && added > 0) {
                    logger.debug("Executing batch: {} rows [{}]", added, tableName)
                    statement.executeBatch()
                    statement.clearBatch()
                    logger.debug("Batch executed [{}]", tableName)
                }
            }
        }
        logger.info("Imported: {}", file)
    }

    private fun getColumns(connection: Connection, tableName: String): Map<Int, Column> {
        val columns = mutableMapOf<Int, Column>()
        var idx = 0
        connection.metaData.getColumns(null, jdbc.schema, tableName, "%").use { rs ->
            while (rs.next()) {
                val type = rs.getInt("DATA_TYPE")
                val name = rs.getString("COLUMN_NAME")
                columns.put(++idx, Column(type, name))
            }
        }
        return columns
    }

    private fun deleteRows(connection: Connection, tableName: String) {
        connection.createStatement().use { statement ->
            val deleted = statement.executeUpdate("DELETE FROM ${jdbc.tableName(tableName)}")
            if (deleted > 0) {
                logger.info("Deleted {} row(s) from {}", deleted, tableName)
            }
        }
    }

    private fun isEmpty(connection: Connection, tableName: String): Boolean {
        connection.createStatement().use { statement ->
            statement.executeQuery("SELECT 1 FROM ${jdbc.tableName(tableName)}").use { rs ->
                return !rs.next()
            }
        }
    }
}
