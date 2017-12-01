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
import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection

internal class Session(private val jdbc: Jdbc) : AutoCloseable {
    val schema = jdbc.schema
    private val dataSource: HikariDataSource

    init {
        val config = HikariConfig()
        config.jdbcUrl = jdbc.url
        config.username = jdbc.username
        config.password = jdbc.password
        this.dataSource = HikariDataSource(config)
    }

    fun isSqlServer(): Boolean {
        return ":sqlserver:" in jdbc.url
    }

    fun tableName(tableName: String): String {
        if (schema.isEmpty()) {
            return quote(tableName)
        } else {
            return quote(schema) + "." + quote(tableName)
        }
    }

    fun quote(str: String): String {
        if (jdbc.quotes) {
            return if (isSqlServer()) "[$str]" else "\"$str\""
        } else {
            return str
        }
    }

    inline fun <T> withConnection(block: (Connection) -> T): T {
        return dataSource.connection.use { connection ->
            try {
                block(connection)
            } catch (t: Throwable) {
                dataSource.evictConnection(connection)
                throw t
            }
        }
    }

    override fun close() {
        dataSource.close()
    }
}
