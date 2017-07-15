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
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.File
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Connection
import java.sql.Date
import java.sql.DriverManager
import java.sql.PreparedStatement
import java.sql.Types
import java.text.SimpleDateFormat
import java.util.Queue

internal class Importer(val jdbc: Jdbc, val deleteBeforeImport: Boolean, val files: Queue<File>) : Runnable {
    companion object {
        val logger = LoggerFactory.getLogger(Importer::class.java)!!

        val BATCH_SIZE = 10000
    }

    override fun run() {
        DriverManager.getConnection(jdbc.url, jdbc.username, jdbc.password).use { connection ->
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
            val data = DataInputStream(input)

            val version = data.readInt()
            if (version != 1) {
                throw IllegalStateException("Unexpected version: $version")
            }

            val exportTableName = data.readUTF()
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

            val columns = mutableMapOf<Int, Column>()
            val columnCount = data.readInt()
            for (idx in 1..columnCount) {
                val type = data.readInt()
                val name = data.readUTF()
                columns.put(idx, Column(type, name))
            }

            val str = StringBuilder()
            str.append("INSERT INTO ")
            str.append(jdbc.tableName(tableName))
            str.append(" (")
            for (idx in 1..columnCount) {
                val column = columns[idx]!!
                if (idx > 1) {
                    str.append(",")
                }
                str.append(column.name)
            }
            str.append(") VALUES (")
            str.append("?")
            str.append(",?".repeat(columnCount - 1))
            str.append(")")

            connection.prepareStatement(str.toString()).use { statement ->
                var added = 0
                while (true) {
                    val prefix = data.read()
                    if (prefix == 0) {
                        break
                    }
                    for (idx in 1..columnCount) {
                        read(data, idx, columns[idx]!!, statement)
                    }
                    statement.addBatch()
                    ++added
                    if (added >= BATCH_SIZE) {
                        statement.executeBatch()
                        statement.clearParameters()
                        added = 0
                    }
                }
                if (added > 0) {
                    statement.executeBatch()
                }
            }
        }
        logger.info("Imported: {}", file)
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

    private fun read(data: DataInputStream, index: Int, column: Column, statement: PreparedStatement) {
        val prefix = data.read()
        if (prefix == 0) {
            statement.setNull(index, column.type)
        } else {
            when (column.type) {
                Types.NUMERIC, Types.DECIMAL, Types.FLOAT -> readBigDecimal(data, index, statement)
                Types.SMALLINT, Types.BIGINT, Types.INTEGER -> readBigInteger(data, index, statement)
                Types.VARCHAR -> readVarchar(data, index, statement)
                Types.BLOB, Types.CLOB -> readBlob(data, index, statement)
                Types.DATE -> readDate(data, false, index, statement)
                Types.TIMESTAMP -> readDate(data, true, index, statement)
                else -> throw IllegalArgumentException("Unknown column type: $column")
            }
        }
    }

    private fun readBigDecimal(data: DataInputStream, index: Int, statement: PreparedStatement) {
        val scale = data.readInt()
        val size = data.readInt()
        val bytes = ByteArray(size)
        data.readFully(bytes)
        statement.setBigDecimal(index, BigDecimal(BigInteger(bytes), scale))
    }

    private fun readBigInteger(data: DataInputStream, index: Int, statement: PreparedStatement) {
        val scale = data.readInt()
        if (scale != 0) {
            throw IllegalStateException("Expected scale 0: $scale")
        }
        val size = data.readInt()
        val bytes = ByteArray(size)
        data.readFully(bytes)
        statement.setBigDecimal(index, BigDecimal(BigInteger(bytes), 0))
    }

    private fun readVarchar(data: DataInputStream, index: Int, statement: PreparedStatement) {
        statement.setString(index, data.readUTF())
    }

    private fun readBlob(data: DataInputStream, index: Int, statement: PreparedStatement) {
        val size = data.readInt()
        val bytes = ByteArray(size)
        data.readFully(bytes)
        statement.setBlob(index, ByteArrayInputStream(bytes))
    }

    private fun readDate(data: DataInputStream, withTime: Boolean, index: Int, statement: PreparedStatement) {
        val format = if (withTime) "yyyy-MM-dd'T'HH:mm:ssZ" else "yyyy-MM-ddZ"
        val date = SimpleDateFormat(format).parse(data.readUTF())
        statement.setDate(index, Date(date.time))
    }
}
