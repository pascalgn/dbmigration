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

import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.Statement
import java.sql.Types
import java.util.TreeMap

internal object Utils {
    private val logger = LoggerFactory.getLogger(Utils::class.java)!!

    fun getColumns(connection: Connection, schema: String, tableName: String): Map<Int, Column> {
        val columns = TreeMap<Int, Column>()
        var idx = 0
        connection.metaData.getColumns(null, schema, tableName, "%").use { rs ->
            while (rs.next()) {
                var type = rs.getInt("DATA_TYPE")
                if (type == Types.TIMESTAMP) {
                    val typeName = rs.getString("TYPE_NAME")
                    if (typeName.startsWith("DATE")) {
                        type = Types.DATE
                    }
                }
                val name = rs.getString("COLUMN_NAME")
                val scale = rs.getInt("DECIMAL_DIGITS")
                val precision = rs.getInt("COLUMN_SIZE")
                val nullable = when (rs.getInt("NULLABLE")) {
                    DatabaseMetaData.columnNullable -> true
                    DatabaseMetaData.columnNoNulls -> false
                    else -> throw IllegalStateException("Cannot read nullable property: $tableName.$name")
                }
                columns.put(++idx, Column(type, name, scale, precision, nullable))
            }
        }
        if (columns.isEmpty()) {
            throw IllegalStateException("Table with 0 columns: $tableName")
        }
        logger.trace("Columns for {}: {}", tableName, columns)
        return columns
    }

    fun isEmpty(session: Session, connection: Connection, tableName: String): Boolean {
        connection.createStatement().use { statement ->
            executeQuery(statement, "SELECT 1 FROM ${session.tableName(tableName)}").use { rs ->
                return !rs.next()
            }
        }
    }

    fun deleteRows(session: Session, connection: Connection, tableName: String) {
        connection.createStatement().use { statement ->
            val deleted = statement.executeUpdate("DELETE FROM ${session.tableName(tableName)}")
            if (deleted > 0) {
                Utils.logger.info("Deleted {} row(s) from {}", deleted, tableName)
            }
        }
    }

    fun rowCount(session: Session, connection: Connection, tableName: String): Long {
        connection.createStatement().use { statement ->
            val sql = "SELECT COUNT(1) FROM ${session.tableName(tableName)}"
            executeQuery(statement, sql).use { rs ->
                if (rs.next()) {
                    val count: Long = rs.getLong(1)
                    if (rs.next()) {
                        throw IllegalStateException("Expected only one row: $sql")
                    }
                    return count
                } else {
                    throw IllegalStateException("No results: $sql")
                }
            }
        }
    }

    private fun executeQuery(statement: Statement, sql: String): ResultSet {
        try {
            return statement.executeQuery(sql)
        } catch (e: Exception) {
            throw IllegalStateException("Error executing query: $sql", e)
        }
    }

    /**
     * @return Mapping source column index to target column index
     */
    fun getMapping(sourceColumns: Map<Int, Column>, targetColumns: Map<Int, Column>): Map<Int, Int> {
        val mapping = mutableMapOf<Int, Int>()
        for ((targetIdx, targetColumn) in targetColumns) {
            for ((sourceIdx, sourceColumn) in sourceColumns) {
                if (sourceColumn.name.toUpperCase() == targetColumn.name.toUpperCase()) {
                    mapping.put(sourceIdx, targetIdx)
                    break
                }
            }
            if (!mapping.containsValue(targetIdx)) {
                throw IllegalStateException("Missing source column for target $targetColumn")
            }
        }
        return mapping
    }
}
