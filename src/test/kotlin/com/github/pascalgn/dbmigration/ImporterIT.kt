package com.github.pascalgn.dbmigration

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.util.LinkedList

class ImporterIT : AbstractIT() {
    @Test
    fun runImport() {
        val pkg = "com/github/pascalgn/dbmigration"
        val jdbcUrl = "jdbc:h2:mem:test;DB_CLOSE_DELAY=10;INIT=RUNSCRIPT FROM 'classpath:$pkg/runImport.sql'"
        val jdbc = Jdbc(jdbcUrl, "", "", "", false)

        val user = File(directory, "User.bin")
        user.outputStream().use {
            javaClass.getResourceAsStream("/$pkg/User.bin").copyTo(it)
        }
        val files = LinkedList<File>(listOf(user))

        Importer(jdbc, true, files).run()

        select("jdbc:h2:mem:test", "SELECT * FROM USER") { rs ->
            assertTrue(rs.next())
            assertEquals(1, rs.getInt(1))
            assertEquals("user1", rs.getString(2))
            assertFalse(rs.next())
        }
    }
}
