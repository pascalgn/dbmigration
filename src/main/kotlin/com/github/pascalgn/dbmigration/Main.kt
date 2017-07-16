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

import com.github.pascalgn.dbmigration.config.Context
import java.io.File
import java.util.Properties

object Main {
    private val DEFAULT_CONFIGURATION = "/com/github/pascalgn/dbmigration/migration-defaults.properties"

    @JvmStatic
    fun main(args: Array<String>) {
        if (args.size != 1) {
            throw IllegalArgumentException("usage: main <directory>")
        }

        val root = File(args[0])
        if (!root.isDirectory) {
            throw IllegalStateException("Not a directory: $root")
        }

        val configuration = File(root, "migration.properties")
        if (!configuration.exists()) {
            if (configuration.createNewFile()) {
                configuration.outputStream().use { out ->
                    Main::class.java.getResourceAsStream(DEFAULT_CONFIGURATION).copyTo(out)
                }
            }
            throw IllegalStateException("No such file, created defaults: $configuration")
        }

        val properties = Properties()
        configuration.reader(Charsets.ISO_8859_1).use { properties.load(it) }

        Migration(Context.fromProperties(root, properties)).run()
    }
}
