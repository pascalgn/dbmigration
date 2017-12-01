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

internal class Filter(private val include: List<String> = emptyList(), private val exclude: List<String> = emptyList()) {
    fun validate(tables: Collection<Table>) {
        val matchesExisting = { pattern: String ->
            if (pattern.contains('.')) {
                val (tableName, columnName) = pattern.split('.', limit = 2)
                tables.any {
                    it.name == tableName && it.columns.values.any { it.name == columnName }
                }
            } else {
                tables.any { it.name == pattern }
            }
        }

        include.filter { !matchesExisting(it) }.forEach {
            throw IllegalStateException("Include does not match any existing tables or columns: ${it}")
        }

        exclude.filter { !matchesExisting(it) }.forEach {
            throw IllegalStateException("Exclude does not match any existing tables or columns: ${it}")
        }
    }

    fun excludeTable(tableName: String): Boolean {
        return (include.isNotEmpty() && !include.contains(tableName)) || (exclude.contains(tableName))
    }

    fun excludeColumn(tableName: String, columnName: String): Boolean {
        return exclude.filter { it.contains('.') }.any {
            val (table, column) = it.split('.', limit = 2)
            table == tableName && column == columnName
        }
    }
}
