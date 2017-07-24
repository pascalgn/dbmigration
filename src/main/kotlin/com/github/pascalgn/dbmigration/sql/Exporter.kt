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

import com.github.pascalgn.dbmigration.config.Jdbc
import com.github.pascalgn.dbmigration.io.BinaryWriter
import org.slf4j.LoggerFactory
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.sql.ResultSet
import java.sql.Types
import java.util.Queue

internal class Exporter(private val outputDir: File, private val jdbc: Jdbc,
                        private val tables: Queue<Table>) : Runnable {
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
                        val writer = BinaryWriter(output)

                        // file format version:
                        writer.writeTableName(tableName)

                        val columnCount = rs.metaData.columnCount
                        val columns = (1..columnCount).associateBy({ it }, { getColumn(rs, it) })

                        if (columnCount == 0) {
                            throw IllegalStateException("Table with 0 columns: $tableName")
                        }

                        writer.writeColumns(columns)

                        rs.fetchSize = FETCH_SIZE
                        while (rs.next()) {
                            writer.writeRow { idx -> read(rs, idx, columns[idx]!!) }
                        }
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

    private fun read(rs: ResultSet, index: Int, column: Column): Any? {
        val value: Any? = when (column.type) {
            Types.NUMERIC, Types.DECIMAL, Types.FLOAT -> rs.getBigDecimal(index)
            Types.SMALLINT, Types.TINYINT, Types.INTEGER -> rs.getInt(index)
            Types.BIGINT -> rs.getBigDecimal(index)
            Types.VARCHAR -> rs.getString(index)
            Types.BLOB, Types.CLOB -> rs.getBinaryStream(index)
            Types.DATE -> rs.getDate(index)
            Types.TIMESTAMP -> rs.getDate(index)
            else -> throw IllegalArgumentException("Unknown column type: $column")
        }
        return if (rs.wasNull()) null else value
    }
}
