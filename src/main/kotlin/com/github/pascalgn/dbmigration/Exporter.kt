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
import java.io.DataOutputStream
import java.io.File
import java.io.InputStream
import java.math.BigDecimal
import java.sql.Connection
import java.sql.Date
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Types
import java.text.SimpleDateFormat
import java.util.Queue

internal class Exporter(val outputDir: File, val jdbc: Jdbc, val tables: Queue<Table>) : Runnable {
    companion object {
        val logger = LoggerFactory.getLogger(Exporter::class.java)!!

        val FETCH_SIZE = 5000
    }

    override fun run() {
        DriverManager.getConnection(jdbc.url, jdbc.username, jdbc.password).use { connection ->
            while (true) {
                val table = tables.poll() ?: break
                exportTable(connection, table)
            }
        }
    }

    private fun exportTable(connection: Connection, table: Table) {
        val tableName = table.name
        val file = File(outputDir, "$tableName.bin")
        if (file.createNewFile()) {
            logger.info("Exporting {}", tableName)
            connection.createStatement().use { statement ->
                statement.fetchSize = FETCH_SIZE
                statement.executeQuery("SELECT * FROM ${jdbc.tableName(tableName)}").use { rs ->
                    file.outputStream().use { output ->
                        val data = DataOutputStream(output)

                        // file format version:
                        data.writeInt(1)

                        data.writeUTF(tableName)

                        val columnCount = rs.metaData.columnCount
                        val columns = (1..columnCount).associateBy({ it }, { getColumn(rs, it) })

                        if (columnCount == 0) {
                            throw IllegalStateException("Table with 0 columns: $tableName")
                        }

                        data.writeInt(columnCount)
                        for (idx in 1..columnCount) {
                            val column = columns[idx]!!
                            data.writeInt(column.type)
                            data.writeUTF(column.name)
                        }

                        rs.fetchSize = FETCH_SIZE
                        while (rs.next()) {
                            data.writeByte(1)
                            for (idx in 1..columnCount) {
                                write(rs, idx, columns[idx]!!, data)
                            }
                        }

                        data.writeByte(0)
                    }
                }
            }
            logger.info("Exported {}", file)
        } else {
            logger.warn("File exists: {}", file)
        }
    }

    private fun getColumn(rs: ResultSet, index: Int): Column {
        var columnType = rs.metaData.getColumnType(index)
        if (columnType == Types.TIMESTAMP) {
            val columnTypeName = rs.metaData.getColumnTypeName(index)
            if (columnTypeName.startsWith("DATE")) {
                columnType = Types.DATE
            }
        }
        val columnName = rs.metaData.getColumnName(index)
        return Column(columnType, columnName)
    }

    private fun write(rs: ResultSet, index: Int, column: Column, data: DataOutputStream) {
        when (column.type) {
            Types.NUMERIC, Types.DECIMAL, Types.FLOAT -> write(rs.getBigDecimal(index), rs.wasNull(), data)
            Types.SMALLINT, Types.BIGINT, Types.INTEGER -> write(rs.getBigDecimal(index), rs.wasNull(), data)
            Types.VARCHAR -> write(rs.getString(index), rs.wasNull(), data)
            Types.BLOB, Types.CLOB -> write(rs.getBinaryStream(index), rs.wasNull(), data)
            Types.DATE -> write(rs.getDate(index), rs.wasNull(), false, data)
            Types.TIMESTAMP -> write(rs.getDate(index), rs.wasNull(), true, data)
            else -> throw IllegalArgumentException("Unknown column type: $column")
        }
    }

    private fun write(num: BigDecimal?, wasNull: Boolean, data: DataOutputStream) {
        if (num == null || wasNull) {
            data.writeByte(0)
        } else {
            data.writeByte(1)
            data.writeInt(num.scale())
            val bytes = num.unscaledValue().toByteArray()
            data.writeInt(bytes.size)
            data.write(bytes)
        }
    }

    private fun write(str: String?, wasNull: Boolean, data: DataOutputStream) {
        if (str == null || wasNull) {
            data.writeByte(0)
        } else {
            data.writeByte(1)
            data.writeUTF(str)
        }
    }

    private fun write(input: InputStream?, wasNull: Boolean, data: DataOutputStream) {
        if (input == null || wasNull) {
            data.writeByte(0)
        } else {
            data.writeByte(1)
            input.use {
                val bytes = it.readBytes()
                data.writeInt(bytes.size)
                data.write(bytes)
            }
        }
    }

    private fun write(date: Date?, wasNull: Boolean, withTime: Boolean, data: DataOutputStream) {
        if (date == null || wasNull) {
            data.writeByte(0)
        } else {
            data.writeByte(1)
            val format = if (withTime) "yyyy-MM-dd'T'HH:mm:ssZ" else "yyyy-MM-ddZ"
            data.writeUTF(SimpleDateFormat(format).format(date))
        }
    }
}
