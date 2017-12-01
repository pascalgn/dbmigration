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
import com.github.pascalgn.dbmigration.sql.Filter
import com.github.pascalgn.dbmigration.sql.JdbcExporter
import com.github.pascalgn.dbmigration.sql.Session
import com.github.pascalgn.dbmigration.sql.Table
import com.github.pascalgn.dbmigration.sql.Utils
import org.apache.commons.lang.exception.ExceptionUtils
import org.slf4j.LoggerFactory
import java.io.File
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.StandardCopyOption
import java.sql.SQLRecoverableException
import java.sql.SQLTransientException

internal class ExportTask(private val outputDir: File, private val overwrite: Boolean, private val filter: Filter,
                          private val table: Table, private val session: Session, private val retries: Int = 0,
                          private val fetchSize: Int = 0) : Task() {
    companion object {
        val logger = LoggerFactory.getLogger(ExportTask::class.java)!!
        private fun recoverable(e: Exception): Boolean {
            return ExceptionUtils.indexOfType(e, SQLRecoverableException::class.java) >= 0
                || ExceptionUtils.indexOfType(e, SQLTransientException::class.java) >= 0
        }
    }

    private val file = File(outputDir, "${table.name}.bin")

    private var start = 0
    private var rowsAdded = false

    override fun doInitialize() {
        if (file.exists() && !overwrite) {
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
        if (overwrite || !file.exists()) {
            // write to the temporary file, then move to the real file after this export is done.
            // otherwise it cannot be determined which files were exported successfully in case of an error
            val tmp = File.createTempFile("tmp-${table.name}-", "", outputDir)

            for (retry in 0..retries) {
                try {
                    BinaryWriter(FileOutputStream(tmp, true)).use {
                        CountingDataWriter(it).use { writer ->
                            val writeHeader = retry == 0
                            JdbcExporter(table, session, filter, writer, outputDir, fetchSize, writeHeader, start).run()
                        }
                    }
                    break
                } catch (e: Exception) {
                    if (recoverable(e) && retry < retries) {
                        logger.warn("Recoverable exception during export: {}: {} ({}/{})", table.name,
                            e.message, retry + 1, retries)
                        logger.info("Retrying export of {}: starting at row {}", table.name, start + 1)
                    } else {
                        if (!tmp.delete()) {
                            logger.warn("Could not delete temporary file: {}", tmp)
                        }
                        if (e is InterruptedException) {
                            Thread.currentThread().interrupt()
                        }
                        throw e
                    }
                }
            }

            if (file.createNewFile()) {
                Files.move(tmp.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING)
            } else {
                logger.warn("Cannot move {}, target exists: {}", tmp, file)
            }

            if (completed < size) {
                // rows could have been deleted while running, still indicate success:
                logger.warn("Rows have been deleted while the export was running: {}", table.name)
                completed = size
            }
            if (rowsAdded) {
                logger.warn("Rows have been added while the export was running: {}", table.name)
            }
        } else {
            logger.warn("File has been created after this task has been initialized: {}", file)
            completed = size
        }
    }

    private inner class CountingDataWriter(private val delegate: DataWriter) : DataWriter {
        override fun setHeader(tableName: String, columns: Map<Int, Column>) {
            delegate.setHeader(tableName, columns)
        }

        override fun writeHeader() {
            delegate.writeHeader()
        }

        override fun writeRow(row: Array<Any?>) {
            delegate.writeRow(row)
            start++
            if (completed < size) {
                completed++
            } else {
                rowsAdded = true
            }
        }

        override fun writeRow(block: (Int) -> Any?) {
            delegate.writeRow(block)
            start++
            if (completed < size) {
                completed++
            } else {
                rowsAdded = true
            }
        }

        override fun close() {
            delegate.close()
        }
    }
}
