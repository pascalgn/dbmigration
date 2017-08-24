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
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Test
import java.io.InputStream
import java.sql.Types

class BinaryReaderTest : AbstractTest() {
    @Test
    fun readV1() {
        openResource("User-v1.bin") { assertRead(it) }
    }

    @Test
    fun readV2() {
        openResource("User-v2.bin") { assertRead(it) }
    }

    private fun assertRead(inputStream: InputStream) {
        val reader = BinaryReader(inputStream)
        assertEquals("User", reader.readTableName())
        val columns = reader.readColumns()
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
}
