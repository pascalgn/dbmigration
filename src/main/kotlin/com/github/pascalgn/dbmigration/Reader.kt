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

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.InputStream
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Date
import java.sql.Types
import java.text.SimpleDateFormat
import java.util.TreeMap

internal class Reader(input: InputStream) {
    private val data = DataInputStream(input)
    private val columns = TreeMap<Int, Column>()

    fun readTableName(): String {
        val version = data.readInt()
        if (version != 1) {
            throw IllegalStateException("Unexpected version: $version")
        }

        return data.readUTF()
    }

    fun readColumns(): Map<Int, Column> {
        columns.clear()
        val columnCount = data.readInt()
        for (idx in 1..columnCount) {
            val type = data.readInt()
            val name = data.readUTF()
            columns.put(idx, Column(type, name))
        }
        return columns
    }

    fun nextRow(): Boolean {
        val prefix = data.read()
        return (prefix == 1)
    }

    inline fun readRow(block: (Int, Any?) -> Any) {
        for (idx in 1..columns.size) {
            val prefix = data.read()
            if (prefix == 0) {
                block.invoke(idx, null)
            } else {
                val column = columns[idx]!!
                when (column.type) {
                    Types.NUMERIC, Types.DECIMAL, Types.FLOAT -> block.invoke(idx, readBigDecimal())
                    Types.SMALLINT, Types.BIGINT, Types.INTEGER -> block.invoke(idx, readBigInteger())
                    Types.VARCHAR -> block.invoke(idx, data.readUTF())
                    Types.BLOB, Types.CLOB -> block.invoke(idx, readBlob())
                    Types.DATE -> block.invoke(idx, readDate(false))
                    Types.TIMESTAMP -> block.invoke(idx, readDate(true))
                    else -> throw IllegalArgumentException("Unknown column type: $column")
                }
            }
        }
    }

    private fun readBigDecimal(): BigDecimal {
        val scale = data.readInt()
        val size = data.readInt()
        val bytes = ByteArray(size)
        data.readFully(bytes)
        return BigDecimal(BigInteger(bytes), scale)
    }

    private fun readBigInteger(): BigDecimal {
        val scale = data.readInt()
        if (scale != 0) {
            throw IllegalStateException("Expected scale 0: $scale")
        }
        val size = data.readInt()
        val bytes = ByteArray(size)
        data.readFully(bytes)
        return BigDecimal(BigInteger(bytes), 0)
    }

    private fun readBlob(): InputStream {
        val size = data.readInt()
        val bytes = ByteArray(size)
        data.readFully(bytes)
        return ByteArrayInputStream(bytes)
    }

    private fun readDate(withTime: Boolean): Date {
        val format = if (withTime) "yyyy-MM-dd'T'HH:mm:ssZ" else "yyyy-MM-ddZ"
        val date = SimpleDateFormat(format).parse(data.readUTF())
        return Date(date.time)
    }
}
