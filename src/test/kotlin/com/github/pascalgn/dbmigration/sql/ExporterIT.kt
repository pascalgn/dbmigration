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
import com.github.pascalgn.dbmigration.io.BinaryWriter
import org.junit.Assert.assertEquals
import org.junit.Test
import java.io.File

class ExporterIT : AbstractIT() {
    @Test
    fun runExport() {
        val jdbcUrl = "jdbc:h2:mem:test;INIT=RUNSCRIPT FROM 'classpath:$PKG/runExport.sql'"
        val jdbc = Jdbc(jdbcUrl, "", "", "", false)
        val file = File(directory, "User.bin")

        Session(jdbc).use {
            BinaryWriter(file.outputStream()).use { writer ->
                Exporter(Table("User", 1L), it, writer).run()
            }
        }

        assertEquals(1, directory.list().size)
        val userExpected = openResource("Export-User.bin") { it.readBytes() }
        val userActual = file.readBytes()
        assertEquals(userExpected.toList(), userActual.toList())
    }
}
