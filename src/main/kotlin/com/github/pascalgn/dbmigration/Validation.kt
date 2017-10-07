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

class Validation(private val files: List<File>) : Runnable {
    companion object {
        val logger = LoggerFactory.getLogger(Validation::class.java)!!
    }

    override fun run() {
        for (file in files) {
            if (file.isDirectory) {
                logger.warn("Skipping directory: {}", file)
                continue
            }
            try {
                var rows = 0
                BinaryReader(file.inputStream()).use {
                    it.readHeader()
                    while (it.nextRow()) {
                        it.readRow { _, _ -> }
                        rows++
                    }
                }
                logger.info("Successfully read {} rows from {}", rows, file.name)
            } catch (e: Exception) {
                if (logger.isDebugEnabled) {
                    logger.error("Error validating file: {}", file, e)
                } else {
                    logger.error("Error validating file: {}: {}", file.name, e.message)
                }
            }
        }
    }
}
