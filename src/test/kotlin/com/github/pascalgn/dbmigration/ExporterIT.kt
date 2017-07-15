package com.github.pascalgn.dbmigration

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.io.File
import java.util.LinkedList

class ExporterIT : AbstractIT() {
    @Test
    fun runExport() {
        val pkg = "com/github/pascalgn/dbmigration"
        val jdbcUrl = "jdbc:h2:mem:test;INIT=RUNSCRIPT FROM 'classpath:$pkg/runExport.sql'"
        val jdbc = Jdbc(jdbcUrl, "", "", "", false)
        val tables = LinkedList<Table>(listOf(Table("User", 1L)))

        Exporter(directory, jdbc, tables).run()

        assertEquals(1, directory.list().size)
        val userExpected = javaClass.getResource("/$pkg/User.bin").readBytes()
        val userActual = File(directory, "User.bin").readBytes()
        assertEquals(userExpected.toList(), userActual.toList())
    }
}
