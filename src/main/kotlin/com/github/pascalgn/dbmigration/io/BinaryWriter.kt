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

package com.github.pascalgn.dbmigration.io

import com.github.pascalgn.dbmigration.sql.Column
import java.io.DataOutputStream
import java.io.InputStream
import java.io.OutputStream
import java.math.BigDecimal
import java.sql.Date
import java.sql.Types
import java.text.SimpleDateFormat

internal open class BinaryWriter(output: OutputStream) : DataWriter {
    private val data = DataOutputStream(output)

    private val columns = mutableMapOf<Int, Column>()

    override fun writeTableName(tableName: String) {
        // version:
        data.writeInt(2)
        data.writeUTF(tableName)
    }

    override fun writeColumns(columns: Map<Int, Column>) {
        this.columns.clear()
        this.columns.putAll(columns)
        data.writeInt(columns.size)
        for (idx in 1..columns.size) {
            val column = columns[idx]!!
            data.writeInt(column.type)
            data.writeUTF(column.name)
        }
    }

    override fun writeRow(row: Array<Any?>) {
        writeRow { row[it - 1] }
    }

    override fun writeRow(block: (Int) -> Any?) {
        data.writeByte(1)
        for (idx in 1..columns.size) {
            val column = columns[idx]!!
            val value = block(idx)
            if (value == null) {
                data.writeByte(0)
            } else {
                data.writeByte(1)
                when (column.type) {
                    Types.NUMERIC, Types.DECIMAL, Types.FLOAT -> write(value as BigDecimal)
                    Types.SMALLINT, Types.TINYINT, Types.INTEGER -> data.writeInt(value as Int)
                    Types.BIGINT -> write(value as BigDecimal)
                    Types.VARCHAR -> data.writeUTF(value as String)
                    Types.BLOB, Types.CLOB -> write(value as InputStream)
                    Types.DATE -> write(value as Date, false)
                    Types.TIMESTAMP -> write(value as Date, true)
                    else -> throw IllegalArgumentException("Unknown column type: $column")
                }
            }
        }
    }

    private fun write(num: BigDecimal) {
        data.writeInt(num.scale())
        val bytes = num.unscaledValue().toByteArray()
        data.writeInt(bytes.size)
        data.write(bytes)
    }

    private fun write(input: InputStream) {
        input.use {
            val bytes = it.readBytes()
            data.writeInt(bytes.size)
            data.write(bytes)
        }
    }

    private fun write(date: Date, withTime: Boolean) {
        val format = if (withTime) "yyyy-MM-dd'T'HH:mm:ssZ" else "yyyy-MM-ddZ"
        data.writeUTF(SimpleDateFormat(format).format(date))
    }

    override fun close() {
        data.close()
    }
}
