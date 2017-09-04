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

import com.github.pascalgn.dbmigration.ImportController
import com.github.pascalgn.dbmigration.config.Context
import com.github.pascalgn.dbmigration.io.BinaryReader
import com.github.pascalgn.dbmigration.sql.JdbcImporter
import com.github.pascalgn.dbmigration.sql.Session
import com.github.pascalgn.dbmigration.sql.SqlServerImporter
import com.github.pascalgn.dbmigration.sql.Utils
import org.apache.commons.io.IOUtils.EOF
import org.apache.commons.io.input.ProxyInputStream
import org.slf4j.LoggerFactory
import java.io.File
import java.io.InputStream

internal class ImportTask(private val context: Context, private val imported: ImportController,
                          private val tableNames: Map<String, String>, private val file: File,
                          private val session: Session) : Task() {
    companion object {
        val logger = LoggerFactory.getLogger(ImportTask::class.java)!!
    }

    private var tableName = ""

    override fun doInitialize() {
        val exportTableName = BinaryReader(file.inputStream()).use { reader -> reader.readTableName() }
        tableName = tableNames.getOrDefault(exportTableName.toUpperCase(), "")
        if (tableName.isEmpty()) {
            logger.warn("Table {} not found, skipping import!", exportTableName)
            size = 0
        } else {
            size = file.length()
        }
    }

    override fun doExecute() {
        CountingStream(file.inputStream()).use {
            BinaryReader(it).use { reader ->
                session.withConnection { connection ->
                    try {
                        if (context.target.deleteBeforeImport) {
                            Utils.deleteRows(session, connection, tableName)
                        } else if (!Utils.isEmpty(session, connection, tableName)) {
                            logger.warn("Table {} not empty, skipping import!", tableName)
                            completed = size
                            return
                        }

                        if (session.isSqlServer()) {
                            SqlServerImporter(reader, session, tableName).run()
                        } else {
                            JdbcImporter(reader, session, tableName, context.target.batchSize).run()
                        }
                    } catch (e: InterruptedException) {
                        throw e
                    } catch (e: Exception) {
                        throw IllegalStateException("Error importing $file", e)
                    }
                }
            }
        }
        imported[file] = true
    }

    private inner class CountingStream(input: InputStream) : ProxyInputStream(input) {
        @Synchronized
        override fun skip(length: Long): Long {
            val skip = super.skip(length)
            completed += skip
            return skip
        }

        @Synchronized
        override fun afterRead(n: Int) {
            if (n != EOF) {
                completed += n.toLong()
            }
        }
    }
}
