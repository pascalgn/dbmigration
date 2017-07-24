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

import com.github.pascalgn.dbmigration.io.CsvReader
import org.slf4j.LoggerFactory
import java.math.BigDecimal

internal class SequenceReset(private val reader: CsvReader, private val session: Session) : Runnable {
    companion object {
        private val logger = LoggerFactory.getLogger(SequenceReset::class.java)!!
    }

    private data class ColumnRef(val table: String, val column: String)

    override fun run() {
        // sequence column -> list of columns that use the sequence
        val sequences = mutableMapOf<String, MutableList<ColumnRef>>()

        while (true) {
            val line = reader.readLine() ?: break
            if (line.size != 3) {
                throw IllegalStateException("Expected 3 columns: $line")
            }
            val columnRef = ColumnRef(line[0], line[1])
            val sequenceName = line[2]
            sequences.getOrPut(sequenceName, { mutableListOf<ColumnRef>() }).add(columnRef)
        }

        if (sequences.isEmpty()) {
            return
        }

        session.withConnection { connection ->
            connection.createStatement().use { statement ->
                for ((sequenceName, columnRefs) in sequences.entries) {
                    var max = BigDecimal.ZERO
                    for ((table, column) in columnRefs) {
                        val sql = "SELECT MAX($column) FROM $table"
                        statement.executeQuery(sql).use { rs ->
                            if (rs.next()) {
                                val value = rs.getBigDecimal(1) ?: BigDecimal.ZERO
                                if (value > max) {
                                    max = value
                                }
                            }
                            if (rs.next()) {
                                throw IllegalStateException("Expected only one row: $sql")
                            }
                        }
                    }

                    max += BigDecimal.ONE

                    statement.execute("ALTER SEQUENCE $sequenceName RESTART WITH $max")

                    logger.info("New value for sequence {}: {}", sequenceName, max)
                }
            }
        }
    }
}
