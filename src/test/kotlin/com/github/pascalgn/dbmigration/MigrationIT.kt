package com.github.pascalgn.dbmigration

import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.File

private val PKG = "com/github/pascalgn/dbmigration"

class MigrationIT : AbstractIT() {
    @Test
    fun runMigration() {

        val exportJdbcUrl = "jdbc:h2:mem:source;INIT=RUNSCRIPT FROM 'classpath:$PKG/runMigration.sql'"
        val exportJdbc = Jdbc(exportJdbcUrl, "", "", "", false)
        val export = Source(1, exportJdbc)

        copyToDirectory("before.sql")
        copyToDirectory("after.sql")

        val importJdbcUrl = "jdbc:h2:mem:target;DB_CLOSE_DELAY=10"
        val importJdbc = Jdbc(importJdbcUrl, "", "", "", false)
        val import = Target(1, true, listOf("before.sql"), listOf("after.sql"), importJdbc)

        val context = Context(directory, emptyList(), emptyList(), export, import)

        Migration(context).run()

        val exportDir = File(directory, "export")
        assertEquals(1, exportDir.listFiles().size)

        select(importJdbcUrl, "SELECT * FROM USER") { rs ->
            Assertions.assertTrue(rs.next())
            assertEquals("user-1", rs.getString(1))
            assertEquals(1, rs.getInt(2))
            Assertions.assertFalse(rs.next())
        }
    }

    private fun copyToDirectory(name: String) {
        val file = File(directory, name)
        file.outputStream().use {
            it.write(javaClass.getResource("/$PKG/$name").readBytes())
        }
    }
}
