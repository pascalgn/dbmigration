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

package com.github.pascalgn.dbmigration.sql

import com.github.pascalgn.dbmigration.config.RoundingMode
import com.github.pascalgn.dbmigration.io.DataReader
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Types

internal class JdbcImporter(private val reader: DataReader, private val session: Session,
                            private val tableName: String, private val batchSize: Int = 10000,
                            private val roundingMode: RoundingMode = RoundingMode.WARN) : Runnable {
    companion object {
        val logger = LoggerFactory.getLogger(JdbcImporter::class.java)!!
    }

    private var rows = 0

    override fun run() {
        rows = 0
        session.withConnection { connection ->
            importFile(connection)
        }
    }

    private fun importFile(connection: Connection) {
        logger.info("Importing {}...", tableName)

        reader.readTableName()

        val sourceColumns = reader.readColumns()
        val targetColumns = Utils.getColumns(connection, session.schema, tableName)

        if (targetColumns.isEmpty()) {
            throw IllegalStateException("No columns found for ${session.schema}.$tableName")
        }

        val mapping = Utils.getMapping(sourceColumns, targetColumns)

        val str = StringBuilder()
        str.append("INSERT INTO ")
        str.append(session.tableName(tableName))
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
                logger.trace("Next row:")
                reader.readRow { sourceIdx, value ->
                    val targetIdx = mapping.getOrDefault(sourceIdx, -1)
                    if (targetIdx != -1) {
                        logger.trace("{}: {}", targetIdx, value)
                        val column = targetColumns[targetIdx]!!
                        when (column.type) {
                            Types.BLOB, Types.CLOB, Types.VARBINARY -> {
                                if (value is InputStream) {
                                    statement.setBinaryStream(targetIdx, value, value.available())
                                } else if (value == null) {
                                    statement.setBinaryStream(targetIdx, null as InputStream?, 0)
                                } else {
                                    throw IllegalStateException("Not a stream: $value")
                                }
                            }
                            else -> {
                                if (value is InputStream) {
                                    statement.setBinaryStream(targetIdx, value, value.available())
                                } else if (value is BigDecimal) {
                                    val rounded = Utils.round(value, column.scale, column.precision)
                                    if (roundingMode != RoundingMode.IGNORE && rounded.compareTo(value) != 0) {
                                        if (roundingMode == RoundingMode.WARN) {
                                            logger.warn("Precision lost: {} != {}", rounded, value)
                                        } else if (roundingMode == RoundingMode.FAIL) {
                                            throw IllegalStateException("Precision lost: $rounded != $value")
                                        }
                                    }
                                    statement.setBigDecimal(targetIdx, rounded)
                                } else {
                                    statement.setObject(targetIdx, value)
                                }
                            }
                        }
                    }
                }
                ++rows
                if (batchSize == 0) {
                    statement.execute()
                    statement.clearParameters()
                    ++added
                } else {
                    statement.addBatch()
                    statement.clearParameters()
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

        logger.info("Imported: {} [{} rows]", tableName, rows)
    }
}
