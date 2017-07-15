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

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.io.File
import java.nio.file.Files
import java.sql.DriverManager
import java.sql.ResultSet

abstract class AbstractIT {
    protected lateinit var directory: File

    @BeforeEach
    fun before() {
        directory = Files.createTempDirectory("root").toFile()
    }

    @AfterEach
    fun after() {
        directory.deleteRecursively()
    }

    fun <T> select(url: String, sql: String, block: (ResultSet) -> T): T {
        DriverManager.getConnection(url, "", "").use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use { rs ->
                    return block.invoke(rs)
                }
            }
        }
    }
}
