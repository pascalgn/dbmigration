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
import java.io.Closeable
import java.sql.Connection
import javax.sql.DataSource

internal class Session(private val jdbc: Jdbc) : AutoCloseable {
    val schema = jdbc.schema
    private val dataSource: DataSource

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
        if (jdbc.quotes) {
            return if (isSqlServer()) "[$tableName]" else "\"$tableName\""
        } else {
            return tableName
        }
    }

    inline fun <T> withConnection(block: (Connection) -> T): T {
        return dataSource.connection.use { connection ->
            if (!connection.autoCommit) {
                connection.autoCommit = true
            }
            block(connection)
        }
    }

    override fun close() {
        if (dataSource is Closeable) {
            dataSource.close()
        }
    }
}
