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

import com.github.pascalgn.dbmigration.io.BinaryReader
import org.slf4j.LoggerFactory
import java.io.File
import java.io.InputStream

class Export(private val files: List<File>) : Runnable {
    companion object {
        val logger = LoggerFactory.getLogger(Validation::class.java)!!
    }

    override fun run() {
        for (file in files) {
            if (file.isDirectory) {
                logger.warn("Skipping directory: {}", file)
                continue
            }
            BinaryReader(file.inputStream()).use {
                val header = it.readHeader()

                println("; ${header.tableName}")
                println(header.columns.values.map { it.name }.joinToString(","))

                while (it.nextRow()) {
                    it.readRow { idx, value ->
                        if (idx > 1) {
                            print(",")
                        }
                        when (value) {
                            is InputStream -> print("[${value.available()} bytes]")
                            null -> {}
                            else -> print(value)
                        }
                    }
                    println()
                }
            }
        }
    }
}
