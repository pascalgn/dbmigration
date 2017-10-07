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

package com.github.pascalgn.dbmigration.sql

import com.github.pascalgn.dbmigration.config.RoundingRule
import org.apache.commons.io.output.NullWriter
import org.slf4j.LoggerFactory
import java.io.BufferedWriter
import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import java.sql.Types

private val DUMMY_WRITER = BufferedWriter(NullWriter())

internal class DecimalHandler(private val roundingRule: RoundingRule = RoundingRule.FAIL,
                              private val writer: BufferedWriter = DUMMY_WRITER) {
    companion object {
        val logger = LoggerFactory.getLogger(DecimalHandler::class.java)!!
        val roundingMode = RoundingMode.HALF_UP
        val sep = ","
    }

    init {
        writer.write("table${sep}column${sep}value${sep}rounded")
        writer.newLine()
    }

    fun convert(tableName: String, column: Column, value: BigDecimal): BigDecimal {
        val rounded = round(column, value)
        if (value.compareTo(rounded) == 0) {
            // no rounding was necessary
            return value
        } else {
            if (writer != DUMMY_WRITER) {
                write(tableName, column, value, rounded)
            }
            if (roundingRule == RoundingRule.IGNORE) {
                return rounded
            }
            val msg = "Precision lost: $tableName.${column.name}: $value -> $rounded"
            when (roundingRule) {
                RoundingRule.WARN -> {
                    logger.warn(msg)
                    return rounded
                }
                RoundingRule.FAIL -> throw IllegalStateException(msg)
                else -> throw IllegalStateException("Invalid rounding rule: $roundingRule")
            }
        }
    }

    private fun round(column: Column, value: BigDecimal): BigDecimal {
        return when (column.type) {
            Types.NUMERIC, Types.DECIMAL -> {
                if (column.precision == 0) {
                    throw IllegalArgumentException("Precision must not be zero: $column")
                }
                value.setScale(column.scale, roundingMode).round(MathContext(column.precision, roundingMode))
            }
            Types.REAL -> value.setScale(7, roundingMode).round(MathContext(7, roundingMode))
            Types.FLOAT, Types.DOUBLE -> value.setScale(15, roundingMode).round(MathContext(15, roundingMode))
            Types.BIT, Types.TINYINT, Types.SMALLINT, Types.INTEGER, Types.BIGINT -> value
            else -> throw IllegalArgumentException("Invalid column type: $column")
        }
    }

    @Synchronized
    private fun write(tableName: String, column: Column, value: BigDecimal, rounded: BigDecimal) {
        writer.write(tableName)
        writer.write(sep)
        writer.write(column.name)
        writer.write(sep)
        writer.write(value.toString())
        writer.write(sep)
        writer.write(rounded.toString())
        writer.newLine()
    }
}
