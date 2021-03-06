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

import com.github.pascalgn.dbmigration.io.BinaryReader
import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy
import com.microsoft.sqlserver.jdbc.SQLServerConnection
import org.slf4j.LoggerFactory
import java.io.InputStream
import java.math.BigDecimal
import java.sql.Types

internal class SqlServerImporter(private val reader: BinaryReader, private val session: Session,
                                 private val tableName: String, private val decimalHandler: DecimalHandler) : Runnable {
    companion object {
        private val logger = LoggerFactory.getLogger(SqlServerImporter::class.java)!!
    }

    private var rows = 0

    override fun run() {
        logger.debug("Importing {}...", tableName)

        val header = reader.readHeader()
        val sourceColumns = header.columns

        val columnInfo = mutableMapOf<Int, Column>()
        rows = 0

        class Record : ISQLServerBulkRecord {
            private val row = Array<Any?>(sourceColumns.size, { _ -> null })

            override fun getColumnOrdinals(): Set<Int> {
                return columnInfo.keys
            }

            override fun next(): Boolean {
                if (reader.nextRow()) {
                    reader.readRow { index, value ->
                        if (value is BigDecimal) {
                            if (index in columnInfo) {
                                row[index - 1] = decimalHandler.convert(tableName, columnInfo[index]!!, value)
                            } else {
                                // no entry in columnInfo, this means the value will not be used anyway!
                                row[index - 1] = null
                            }
                        } else if (value is InputStream) {
                            val type = sourceColumns[index]!!.type
                            if (type == Types.CLOB) {
                                row[index - 1] = object {
                                    override fun toString(): String {
                                        return value.bufferedReader().use { it.readText() }
                                    }
                                }
                            } else {
                                row[index - 1] = value
                            }
                        } else {
                            row[index - 1] = value
                        }
                    }
                    if (logger.isTraceEnabled) {
                        logger.trace("Next row:")
                        for (idx in 1..row.size) {
                            logger.trace("{}: {}: {}", idx, row[idx - 1]?.javaClass?.simpleName, row[idx - 1])
                        }
                    }
                    ++rows
                    return true
                } else {
                    return false
                }
            }

            override fun getColumnName(index: Int): String {
                return sourceColumns[index]!!.name
            }

            override fun getColumnType(index: Int): Int {
                val type = sourceColumns[index]!!.type
                return when (type) {
                    Types.CLOB -> Types.VARCHAR
                    Types.BLOB -> Types.VARBINARY
                    Types.FLOAT -> Types.DECIMAL
                    else -> type
                }
            }

            override fun getPrecision(index: Int): Int {
                return columnInfo[index]!!.precision
            }

            override fun getScale(index: Int): Int {
                return columnInfo[index]!!.scale
            }

            override fun isAutoIncrement(index: Int): Boolean {
                return false
            }

            override fun getRowData(): Array<Any?> {
                return row
            }
        }

        session.withConnection { connection ->
            val targetColumns = Utils.getColumns(connection, session.schema, tableName)
            val mapping = Utils.getMapping(sourceColumns, targetColumns)

            connection.createStatement().use { statement ->
                // we don't want any rows, only the metadata!
                statement.executeQuery("SELECT * FROM ${session.tableName(tableName)} WHERE 1=0").use { rs ->
                    for (index in 1..rs.metaData.columnCount) {
                        val type = rs.metaData.getColumnType(index)
                        val name = rs.metaData.getColumnName(index)
                        val scale = rs.metaData.getScale(index)
                        val precision = rs.metaData.getPrecision(index)
                        for ((sourceIndex, targetIndex) in mapping) {
                            if (index == targetIndex) {
                                columnInfo[sourceIndex] = Column(type, name, scale, precision)
                                break
                            }
                        }
                    }
                }
                logger.trace("Column info: {}", columnInfo)
            }

            // SQLServerBulkCopy needs the original SQLServerConnection:
            val sqlServerConnection = connection.unwrap(SQLServerConnection::class.java)
            val bulkCopy = SQLServerBulkCopy(sqlServerConnection)
            bulkCopy.destinationTableName = session.tableName(tableName)
            bulkCopy.bulkCopyOptions.bulkCopyTimeout = Int.MAX_VALUE

            for ((sourceIndex, targetIndex) in mapping) {
                bulkCopy.addColumnMapping(sourceIndex, targetIndex)
            }

            try {
                bulkCopy.writeToServer(Record())
            } catch (t: Throwable) {
                throw IllegalStateException("Error importing $tableName", t)
            }
        }

        logger.debug("Imported: {} [{} rows]", tableName, rows)
    }
}
