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
import org.junit.Assert
import org.junit.Test
import java.math.BigDecimal
import java.sql.Types

class DecimalHandlerTest {
    @Test
    fun testRound1a() {
        checkEquals(BigDecimal("0.01"), BigDecimal("0.005"), 2, 10)
    }

    @Test
    fun testRound1b() {
        checkEquals(BigDecimal("12300"), BigDecimal("12345"), 0, 3)
    }

    @Test
    fun testRound1c() {
        checkEquals(BigDecimal("12300"), BigDecimal("12345"), 2, 3)
    }

    @Test
    fun testRound1d() {
        checkEquals(BigDecimal("200"), BigDecimal("150"), 0, 1)
    }

    @Test
    fun testRound1e() {
        checkEquals(BigDecimal("0"), BigDecimal("0"), 5, 1)
    }

    @Test
    fun testRound1f() {
        checkEquals(BigDecimal("1.01"), BigDecimal("1.01"), 3, 3)
    }

    private fun checkEquals(expected: BigDecimal, input: BigDecimal, scale: Int, precision: Int) {
        val column = Column(Types.NUMERIC, "COLUMN", scale, precision)
        val converted = DecimalHandler(RoundingRule.WARN).convert("TABLE", column, input)
        Assert.assertEquals(expected.toPlainString(), converted.toPlainString())
    }
}
