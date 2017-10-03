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

import org.junit.Assert
import org.junit.Test
import java.math.BigDecimal

class UtilsTest {
    @Test
    fun testRound1a() {
        Assert.assertEquals(BigDecimal("0.01"), Utils.round(BigDecimal("0.005"), 2, 10))
    }

    @Test
    fun testRound1b() {
        Assert.assertEquals(BigDecimal("12300"), Utils.round(BigDecimal("12345"), 0, 3))
    }

    @Test
    fun testRound1c() {
        Assert.assertEquals(BigDecimal("12300.00"), Utils.round(BigDecimal("12345"), 2, 3))
    }

    @Test
    fun testRound1d() {
        Assert.assertEquals(BigDecimal("200"), Utils.round(BigDecimal("150"), 0, 1))
    }

    @Test
    fun testRound1e() {
        Assert.assertEquals(BigDecimal("0.00000"), Utils.round(BigDecimal("0"), 5, 0))
    }
}
