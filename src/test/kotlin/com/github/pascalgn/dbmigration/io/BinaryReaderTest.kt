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

import com.github.pascalgn.dbmigration.AbstractTest
import com.github.pascalgn.dbmigration.sql.Column
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.sql.Date
import java.sql.Timestamp
import java.sql.Types

class BinaryReaderTest : AbstractTest() {
    @Test
    fun readV1() {
        openResource("User-v1.bin") { assertRead12(it) }
    }

    @Test
    fun readV2() {
        openResource("User-v2.bin") { assertRead12(it) }
    }

    @Test
    fun readV3a() {
        openResource("User-v3.bin") { assertRead3(it) }
    }

    @Test
    fun readV3b_writtenInTwoSteps() {
        val columns = mutableMapOf<Int, Column>()
        columns[1] = Column(Types.INTEGER, "ID")
        columns[2] = Column(Types.VARCHAR, "NAME")
        columns[3] = Column(Types.DATE, "DATE")
        columns[4] = Column(Types.TIMESTAMP, "TS")

        val row = Array<Any?>(4, { null })
        row[0] = 1
        row[1] = "user1"
        row[2] = Date(999999)
        row[3] = Timestamp(999999)

        val data = ByteArrayOutputStream().use { output ->
            BinaryWriter(output).use {
                it.setHeader("User", columns)
                it.writeHeader()
            }
            BinaryWriter(output).use {
                it.setHeader("User", columns)
                it.writeRow(row)
            }
            BinaryWriter(output).use {
                it.setHeader("User", columns)
                it.writeRow(row)
            }
            output.toByteArray()
        }

        ByteArrayInputStream(data).use { assertRead3(it, 2) }
    }

    private fun assertRead12(inputStream: InputStream) {
        val reader = BinaryReader(inputStream)
        val header = reader.readHeader()
        assertEquals("User", header.tableName)

        val columns = header.columns
        assertEquals(2, columns.size)
        assertEquals(Types.INTEGER, columns[1]!!.type)
        assertEquals("ID", columns[1]!!.name)
        assertEquals(Types.VARCHAR, columns[2]!!.type)
        assertEquals("NAME", columns[2]!!.name)

        assertTrue(reader.nextRow())
        reader.readRow { index, value ->
            when (index) {
                1 -> assertEquals(1, value)
                2 -> assertEquals("user1", value)
                else -> fail("Unexpected index: $index")
            }
        }
        assertFalse(reader.nextRow())
    }

    private fun assertRead3(inputStream: InputStream, rows: Int = 1) {
        val reader = BinaryReader(inputStream)
        val header = reader.readHeader()
        assertEquals("User", header.tableName)

        val columns = header.columns
        assertEquals(4, columns.size)
        assertEquals(Types.INTEGER, columns[1]!!.type)
        assertEquals("ID", columns[1]!!.name)
        assertEquals(Types.VARCHAR, columns[2]!!.type)
        assertEquals("NAME", columns[2]!!.name)
        assertEquals(Types.DATE, columns[3]!!.type)
        assertEquals("DATE", columns[3]!!.name)
        assertEquals(Types.TIMESTAMP, columns[4]!!.type)
        assertEquals("TS", columns[4]!!.name)

        for (row in 1..rows) {
            assertTrue(reader.nextRow())
            reader.readRow { index, value ->
                when (index) {
                    1 -> assertEquals(1, value)
                    2 -> assertEquals("user1", value)
                    3 -> assertEquals(Date(999999), value)
                    4 -> assertEquals(Timestamp(999999), value)
                    else -> fail("Unexpected index: $index")
                }
            }
        }
        assertFalse(reader.nextRow())
    }
}
