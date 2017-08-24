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
import com.github.pascalgn.dbmigration.io.BinaryReader
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class JdbcImporterIT : AbstractIT() {
    @Test
    fun runImport() {
        val jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=10;INIT=RUNSCRIPT FROM 'classpath:$PKG/runImport.sql'"
        val jdbc = Jdbc(jdbcUrl, "", "", "", false)

        openResource("User-v2.bin") { input ->
            BinaryReader(input).use { reader ->
                Session(jdbc).use { session ->
                    JdbcImporter(reader, session, "USER", 10000).run()
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
}
