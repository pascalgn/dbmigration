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

import java.io.File

internal class FileImportController(private val file: File) : ImportController, AutoCloseable {
    private val imported = LinkedHashSet<String>()

    init {
        if (file.isFile) {
            imported.addAll(file.readLines())
        }
    }

    fun imported(): Set<String> = imported

    @Synchronized
    override fun get(key: File): Boolean = imported.contains(key.name)

    @Synchronized
    override fun set(key: File, value: Boolean) {
        if (value) {
            imported.add(key.name)
        } else {
            imported.remove(key.name)
        }
    }

    @Synchronized
    override fun close() {
        file.bufferedWriter().use { writer ->
            imported.forEach {
                writer.write(it)
                writer.newLine()
            }
        }
    }
}
