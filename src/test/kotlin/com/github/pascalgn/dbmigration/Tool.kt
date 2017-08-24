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
import java.io.File

object Tool {
    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size != 2 || args[0] != "csv") {
            throw IllegalArgumentException("usage: tool csv <file>")
        }

        val file = File(args[1])
        file.inputStream().use { input ->
            val reader = BinaryReader(input)

            val tableName = reader.readTableName()
            println("; Table: $tableName")

            val columns = reader.readColumns()
            for ((type, name) in columns.values) {
                print("$name[$type];")
            }
            println()

            while (reader.nextRow()) {
                reader.readRow { _, value -> print("$value;") }
                println()
            }
        }
    }
}
