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

data class Target(val skip: Boolean, val threads: Int, val deleteBeforeImport: Boolean,
                  val before: Scripts, val after: Scripts, val batchSize: Int,
                  val jdbc: Jdbc, val resetSequences: String, val roundingRule: RoundingRule)

data class Scripts(val files: List<String>, val continueOnError: Boolean = false)

/**
 * Specifies what should happen if a number has to be rounded to fit into the target column
 */
enum class RoundingRule {
    IGNORE, WARN, FAIL
}
