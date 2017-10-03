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

package com.github.pascalgn.dbmigration.config

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

            val sourceSkip = properties.getProperty("source.skip", "false").toBoolean()
            val sourceOverwrite = properties.getProperty("source.overwrite", "false").toBoolean()
            val sourceThreads = properties.getProperty("source.threads", DEFAULT_THREADS).toInt()
            val sourceInclude = properties.getProperty("source.include", "").split(",").filter { it.isNotBlank() }
            val sourceExclude = properties.getProperty("source.exclude", "").split(",").filter { it.isNotBlank() }
            val sourceJdbc = Jdbc(properties.getProperty("source.jdbc.url"),
                properties.getProperty("source.jdbc.username"),
                properties.getProperty("source.jdbc.password"),
                properties.getProperty("source.jdbc.schema"),
                properties.getProperty("source.jdbc.quotes", "true").toBoolean())
            val source = Source(sourceSkip, sourceOverwrite, sourceThreads, sourceInclude, sourceExclude, sourceJdbc)

            val targetSkip = properties.getProperty("target.skip", "false").toBoolean()
            val targetThreads = properties.getProperty("target.threads", DEFAULT_THREADS).toInt()
            val deleteBeforeImport = properties.getProperty("target.deleteBeforeImport", "false").toBoolean()
            val before = Scripts(properties.getProperty("target.before", "").split(",").filter { it.isNotBlank() },
                properties.getProperty("target.before.continueOnError", "false").toBoolean())
            val after = Scripts(properties.getProperty("target.after", "").split(",").filter { it.isNotBlank() },
                properties.getProperty("target.after.continueOnError", "false").toBoolean())
            val batchSize = properties.getProperty("target.batchSize", "10000").toInt()
            val targetJdbc = Jdbc(properties.getProperty("target.jdbc.url"),
                properties.getProperty("target.jdbc.username"),
                properties.getProperty("target.jdbc.password"),
                properties.getProperty("target.jdbc.schema"),
                properties.getProperty("target.jdbc.quotes", "true").toBoolean())
            val resetSequences = properties.getProperty("target.resetSequences", "").trim()
            val roundingMode = RoundingMode.valueOf(properties.getProperty("target.roundingMode", "warn").toUpperCase())
            val target = Target(targetSkip, targetThreads, deleteBeforeImport, before, after, batchSize, targetJdbc,
                resetSequences, roundingMode)

            return Context(root, classpath, drivers, source, target)
        }
    }
}
