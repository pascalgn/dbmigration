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

import com.github.pascalgn.dbmigration.config.Jdbc
import java.sql.Connection
import java.sql.DriverManager

internal class Session(val jdbc: Jdbc) : AutoCloseable {
    val schema = jdbc.schema

    fun isSqlServer(): Boolean {
        return ":sqlserver:" in jdbc.url
    }

    fun tableName(tableName: String): String {
        if (jdbc.quotes) {
            return if (isSqlServer()) "[$tableName]" else "\"tableName\""
        } else {
            return tableName
        }
    }

    inline fun <T> withConnection(block: (Connection) -> T): T {
        return DriverManager.getConnection(jdbc.url, jdbc.username, jdbc.password).use { connection ->
            if (!connection.autoCommit) {
                connection.autoCommit = true
            }
            block(connection)
        }
    }

    override fun close() {
        // currently nothing to do
    }
}
