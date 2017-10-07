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

import com.github.pascalgn.dbmigration.io.DataWriter
import org.slf4j.LoggerFactory
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.File
import java.io.InputStream
import java.sql.Blob
import java.sql.Clob
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Types

/**
 * @param start The first row to export from the result set, default is 0
 */
internal class JdbcExporter(private val table: Table, private val session: Session,
                            private val writer: DataWriter, private val tempDir: File,
                            private val fetchSize: Int = 0, private val writeHeader: Boolean = true,
                            private val start: Int = 0) : Runnable {
    companion object {
        val logger = LoggerFactory.getLogger(JdbcExporter::class.java)!!

        // Streams bigger than this will be written to temporary files instead of being kept in memory
        val MAX_BUFFER_SIZE = 100 * 1024 * 1024
    }

    init {
        if (writeHeader && start > 0) {
            throw IllegalArgumentException("writeHeader cannot be true when start > 0: $start")
        }
    }

    override fun run() {
        session.withConnection { connection ->
            exportTable(connection, table)
        }
    }

    private fun exportTable(connection: Connection, table: Table) {
        val tableName = table.name
        logger.debug("Exporting {}", tableName)
        val stmt = when (start) {
            0 -> connection.createStatement()
            else -> connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY)
        }
        stmt.use { statement ->
            statement.fetchSize = fetchSize
            statement.executeQuery("SELECT * FROM ${session.tableName(tableName)}").use { rs ->
                val columnCount = rs.metaData.columnCount
                val columns = (1..columnCount).associateBy({ it }, { getColumn(rs, it) })

                if (columnCount == 0) {
                    throw IllegalStateException("Table with 0 columns: $tableName")
                }

                logger.trace("Columns for {}: {}", tableName, columns)

                writer.setHeader(tableName, columns)
                if (writeHeader) {
                    writer.writeHeader()
                }

                if (start > 0) {
                    if (!rs.absolute(start)) {
                        throw IllegalStateException("Could not move cursor to row $start: $tableName")
                    }
                }

                val row = Array<Any?>(columnCount, { null })

                rs.fetchSize = fetchSize
                while (rs.next()) {
                    logger.trace("Next row:")
                    for (idx in 1..columnCount) {
                        val value = read(rs, idx, columns[idx]!!)
                        logger.trace("{}: {}: {}", idx, value?.javaClass?.simpleName, value)
                        row[idx - 1] = value
                    }

                    // only write output after all values have been fetched, otherwise
                    // we might write an incomplete row in case of an error
                    writer.writeRow(row)
                }
            }
        }
        logger.debug("Exported {}", tableName)
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
        val scale = rs.metaData.getScale(index)
        val precision = rs.metaData.getPrecision(index)
        return Column(columnType, columnName, scale, precision)
    }

    private fun read(rs: ResultSet, index: Int, column: Column): Any? {
        val value: Any? = when (column.type) {
            Types.NUMERIC, Types.DECIMAL, Types.FLOAT -> rs.getBigDecimal(index)
            Types.SMALLINT, Types.TINYINT, Types.INTEGER -> rs.getInt(index)
            Types.BIGINT -> rs.getBigDecimal(index)
            Types.VARCHAR -> rs.getString(index)
            Types.BLOB -> readStream(rs, rs.getBlob(index))
            Types.CLOB -> readStream(rs, rs.getClob(index))
            Types.DATE -> rs.getDate(index)
            Types.TIMESTAMP -> rs.getTimestamp(index)
            else -> throw IllegalArgumentException("Unknown column type: $column")
        }
        return if (value == null || rs.wasNull()) null else value
    }

    /**
     * Special handling to make sure the whole stream has been read before passing it to the caller
     */
    private fun readStream(rs: ResultSet, blob: Any?): InputStream? {
        if (blob == null || rs.wasNull()) {
            return null
        }
        try {
            val length = when (blob) {
                is Blob -> blob.length()
                is Clob -> blob.length()
                else -> throw IllegalStateException("Invalid object: $blob")
            }
            val inputStream = when (blob) {
                is Blob -> blob.binaryStream
                is Clob -> blob.asciiStream
                else -> throw IllegalStateException("Invalid object: $blob")
            }
            if (length > MAX_BUFFER_SIZE) {
                // write the blob to a temporary file
                val temp = File.createTempFile("tmp-${table.name}-stream-", "", tempDir)
                logger.debug("Blob exceeded buffer size, writing to temporary file: {}", temp.name)
                temp.deleteOnExit()
                temp.outputStream().use { output ->
                    inputStream.use { input ->
                        input.copyTo(output)
                    }
                }
                return temp.inputStream()
            } else {
                // the content is small enough to keep in memory
                NoCopyByteArrayOutputStream(length.toInt()).use { buffer ->
                    inputStream.use { input ->
                        input.copyTo(buffer)
                    }
                    return ByteArrayInputStream(buffer.toByteArray())
                }
            }
        } finally {
            when (blob) {
                is Blob -> blob.free()
                is Clob -> blob.free()
                else -> throw IllegalStateException("Invalid object: $blob")
            }
        }
    }

    /**
     * ByteArrayOutputStream that does not copy the byte array
     */
    private class NoCopyByteArrayOutputStream(length: Int) : ByteArrayOutputStream(length) {
        override fun toByteArray(): ByteArray {
            return buf
        }
    }
}
