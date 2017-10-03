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

import com.github.pascalgn.dbmigration.AbstractIT
import com.github.pascalgn.dbmigration.config.Jdbc
import com.github.pascalgn.dbmigration.config.RoundingMode
import com.github.pascalgn.dbmigration.io.BinaryReader
import com.github.pascalgn.dbmigration.io.DataReader
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test
import java.math.BigDecimal

class JdbcImporterIT : AbstractIT() {
    @Test
    fun runImport1() {
        val jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=10;INIT=RUNSCRIPT FROM 'classpath:$PKG/runImport-1.sql'"
        val jdbc = Jdbc(jdbcUrl, "", "", "", false)

        openResource("User-v2.bin") { input ->
            BinaryReader(input).use { reader ->
                Session(jdbc).use { session ->
                    JdbcImporter(reader, session, "USER").run()
                }
            }
        }

        select("jdbc:h2:mem:test", "SELECT * FROM USER") { rs ->
            assertTrue(rs.next())
            assertEquals(1, rs.getInt(1))
            assertEquals("user1", rs.getString(2))
            assertFalse(rs.next())
        }
    }

    @Test(expected = IllegalStateException::class)
    fun runImport2a() {
        val jdbcUrl = "jdbc:h2:mem:runImport2a;INIT=RUNSCRIPT FROM 'classpath:$PKG/runImport-2.sql'"
        val jdbc = Jdbc(jdbcUrl, "", "", "", false)

        val columns = mapOf(1 to Column(1, "id"), 2 to Column(2, "price"))
        val rows = listOf(mapOf(1 to 123, 2 to BigDecimal("100.0000987654321")))

        val reader = DummyReader("PRICE", columns, rows)

        Session(jdbc).use { session ->
            JdbcImporter(reader, session, "PRICE", 0, RoundingMode.FAIL).run()
        }
    }

    @Test
    fun runImport2b() {
        val jdbcUrl = "jdbc:h2:mem:runImport2b;DB_CLOSE_DELAY=10;INIT=RUNSCRIPT FROM 'classpath:$PKG/runImport-2.sql'"
        val jdbc = Jdbc(jdbcUrl, "", "", "", false)

        val columns = mapOf(1 to Column(1, "id"), 2 to Column(2, "price"))
        val rows = listOf(mapOf(1 to 123, 2 to BigDecimal("100.0000987654321")))

        val reader = DummyReader("PRICE", columns, rows)

        Session(jdbc).use { session ->
            JdbcImporter(reader, session, "PRICE").run()
        }

        select("jdbc:h2:mem:runImport2b", "SELECT * FROM Price") { rs ->
            assertTrue(rs.next())
            assertEquals(123, rs.getInt(1))
            val price = rs.getBigDecimal(2)
            assertEquals("Unexpected price: $price", 0, BigDecimal("100.0001").compareTo(price))
            assertFalse(rs.next())
        }
    }

    private class DummyReader(private val tableName: String, private val columns: Map<Int, Column>,
                              rows: List<Map<Int, Any?>>) : DataReader {
        private val iterator = rows.iterator()

        override fun readTableName(): String {
            return tableName
        }

        override fun readColumns(): Map<Int, Column> {
            return columns
        }

        override fun nextRow(): Boolean {
            return iterator.hasNext()
        }

        override fun readRow(block: (Int, Any?) -> Unit) {
            val row = iterator.next()
            for ((key, value) in row) {
                block(key, value)
            }
        }

        override fun close() {
            // nothing to do
        }
    }
}
