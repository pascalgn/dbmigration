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

package com.github.pascalgn.dbmigration.task

import com.github.pascalgn.dbmigration.io.BinaryWriter
import com.github.pascalgn.dbmigration.io.DataWriter
import com.github.pascalgn.dbmigration.sql.Column
import com.github.pascalgn.dbmigration.sql.Exporter
import com.github.pascalgn.dbmigration.sql.Session
import com.github.pascalgn.dbmigration.sql.Table
import com.github.pascalgn.dbmigration.sql.Utils
import org.slf4j.LoggerFactory
import java.io.File

internal class ExportTask(outputDir: File, private val table: Table, private val session: Session) : Task() {
    companion object {
        val logger = LoggerFactory.getLogger(ExportTask::class.java)!!
    }

    private val file = File(outputDir, "${table.name}.bin")

    override fun doInitialize() {
        if (file.exists()) {
            logger.debug("File exists: {}", file)
            size = 0
        } else {
            logger.debug("{}: counting rows", table.name)
            session.withConnection {
                size = Utils.rowCount(session, it, table.name)
            }
            logger.debug("{}: {} rows", table.name, size)
        }
    }

    override fun doExecute() {
        if (file.createNewFile()) {
            BinaryWriter(file.outputStream()).use {
                CountingDataWriter(it).use { writer ->
                    Exporter(table, session, writer).run()
                }
            }
        } else {
            logger.warn("File has been created after this task has been initialized: {}", file)
            completed = size
        }
    }

    private inner class CountingDataWriter(private val delegate: DataWriter) : DataWriter {
        override fun writeTableName(tableName: String) {
            delegate.writeTableName(tableName)
        }

        override fun writeColumns(columns: Map<Int, Column>) {
            delegate.writeColumns(columns)
        }

        override fun writeRow(row: Array<Any?>) {
            delegate.writeRow(row)
            completed++
        }

        override fun writeRow(block: (Int) -> Any?) {
            delegate.writeRow(block)
            completed++
        }

        override fun close() {
            delegate.close()
        }
    }
}
