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
import org.junit.Assert.assertTrue
import org.junit.Assume.assumeFalse
import org.junit.Test

class SqlServerImporterIT : AbstractIT() {
    @Test
    fun bulkImport() {
        val host = System.getProperty("sqlserver.host")
        val port = System.getProperty("sqlserver.port")

        assumeFalse("Sql server host not set: $host", host.isNullOrBlank())
        assumeFalse("Sql server port not set: $port", port.isNullOrBlank())

        val url = "jdbc:sqlserver://$host:$port;database=dbmigration"
        val jdbc = Jdbc(url, "dbmigration", "dbmigration", "dbmigration", true)

        Session(jdbc).use { session ->
            openResource("beforeSqlServer.sql") { input ->
                val sql = input.bufferedReader().use { it.readText() }
                session.withConnection { connection ->
                    connection.createStatement().use { statement ->
                        statement.execute(sql)
                    }
                }
            }

            openResource("User-v2.bin") { input ->
                BinaryReader(input).use { reader ->
                    SqlServerImporter(reader, session, "User").run()
                }
            }

            session.withConnection { connection ->
                connection.createStatement().use { statement ->
                    statement.executeQuery("SELECT * FROM [User]").use { rs ->
                        assertTrue(rs.next())
                    }
                }
            }
        }
    }
}
