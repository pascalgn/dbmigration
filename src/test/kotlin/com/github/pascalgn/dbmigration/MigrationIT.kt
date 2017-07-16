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

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.File

private val PKG = "com/github/pascalgn/dbmigration"

class MigrationIT : AbstractIT() {
    private val sourceJdbcUrl = "jdbc:h2:mem:source;INIT=RUNSCRIPT FROM 'classpath:$PKG/runMigration.sql'"
    private val targetJdbcUrl = "jdbc:h2:mem:target;DB_CLOSE_DELAY=10"

    @Test
    fun runMigration() {
        val context = createContext()

        Migration(context).run()

        val exportDir = File(directory, "export")
        assertEquals(1, exportDir.listFiles().size)

        select(targetJdbcUrl, "SELECT * FROM USER") { rs ->
            Assertions.assertTrue(rs.next())
            assertEquals("user-1", rs.getString(1))
            assertEquals(1, rs.getInt(2))
            Assertions.assertFalse(rs.next())
        }
    }

    private fun createContext(): Context {
        val sourceJdbc = Jdbc(sourceJdbcUrl, "", "", "", false)
        val source = Source(1, listOf("USERGROUP"), sourceJdbc)

        copyToDirectory("before.sql")
        copyToDirectory("after.sql")

        val targetJdbc = Jdbc(targetJdbcUrl, "", "", "", false)
        val target = Target(1, true, listOf("before.sql"), listOf("after.sql"), targetJdbc)

        val context = Context(directory, emptyList(), emptyList(), source, target)
        return context
    }

    private fun copyToDirectory(name: String) {
        val file = File(directory, name)
        file.outputStream().use {
            it.write(javaClass.getResource("/$PKG/$name").readBytes())
        }
    }
}