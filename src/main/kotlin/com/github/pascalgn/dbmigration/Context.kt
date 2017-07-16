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

import java.io.File
import java.util.Properties

data class Context(val root: File,
                   val classpath: List<String>, val drivers: List<String>,
                   val source: Source, val target: Target) {
    companion object {
        private val DEFAULT_THREADS = "1"

        fun fromProperties(root: File, properties: Properties): Context {
            val classpath = properties.getProperty("classpath", "").split(",").filter { it.isNotBlank() }
            val drivers = properties.getProperty("drivers", "").split(",").filter { it.isNotBlank() }

            val sourceThreads = properties.getProperty("source.threads", DEFAULT_THREADS).toInt()
            val sourceExclude = properties.getProperty("source.exclude", "").split(",").filter { it.isNotBlank() }
            val sourceJdbc = Jdbc(properties.getProperty("source.jdbc.url"),
                properties.getProperty("source.jdbc.username"),
                properties.getProperty("source.jdbc.password"),
                properties.getProperty("source.jdbc.schema"),
                properties.getProperty("source.jdbc.quotes", "true").toBoolean())
            val source = Source(sourceThreads, sourceExclude, sourceJdbc)

            val targetThreads = properties.getProperty("target.threads", DEFAULT_THREADS).toInt()
            val deleteBeforeImport = properties.getProperty("target.deleteBeforeImport", "false").toBoolean()
            val before = properties.getProperty("target.before", "").split(",").filter { it.isNotBlank() }
            val after = properties.getProperty("target.after", "").split(",").filter { it.isNotBlank() }
            val targetJdbc = Jdbc(properties.getProperty("target.jdbc.url"),
                properties.getProperty("target.jdbc.username"),
                properties.getProperty("target.jdbc.password"),
                properties.getProperty("target.jdbc.schema"),
                properties.getProperty("target.jdbc.quotes", "true").toBoolean())
            val target = Target(targetThreads, deleteBeforeImport, before, after, targetJdbc)

            return Context(root, classpath, drivers, source, target)
        }
    }
}
