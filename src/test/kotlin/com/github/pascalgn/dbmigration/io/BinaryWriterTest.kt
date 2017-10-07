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
import com.github.pascalgn.dbmigration.toHex
import org.junit.Assert.assertEquals
import org.junit.Assert.fail
import org.junit.Test
import java.io.ByteArrayOutputStream
import java.sql.Date
import java.sql.Timestamp
import java.sql.Types

class BinaryWriterTest : AbstractTest() {
    @Test
    fun writeV3() {
        val columns = mutableMapOf<Int, Column>()
        columns[1] = Column(Types.INTEGER, "ID")
        columns[2] = Column(Types.VARCHAR, "NAME")
        columns[3] = Column(Types.DATE, "DATE")
        columns[4] = Column(Types.TIMESTAMP, "TS")

        val actual = ByteArrayOutputStream().use { output ->
            BinaryWriter(output).use { writer ->
                writer.setHeader("User", columns)
                writer.writeHeader()
                writer.writeRow { index ->
                    when (index) {
                        1 -> 1
                        2 -> "user1"
                        3 -> Date(999999)
                        4 -> Timestamp(999999)
                        else -> fail("Unexpected index: $index")
                    }
                }
            }
            output.toByteArray()
        }

        val expected = openResource("User-v3.bin") { it.readBytes() }

        assertEquals("Byte arrays differ!", expected.toHex(), actual.toHex())
    }
}
