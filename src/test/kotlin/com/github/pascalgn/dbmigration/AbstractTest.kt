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

import java.io.InputStream

abstract class AbstractTest {
    companion object {
        val PKG = "com/github/pascalgn/dbmigration"
    }

    inline fun <T> openResource(name: String, block: (InputStream) -> T): T {
        val stream = javaClass.getResourceAsStream("$PKG/$name")
            ?: javaClass.getResourceAsStream("/$PKG/$name")
            ?: Thread.currentThread().contextClassLoader.getResourceAsStream("$PKG/$name")
            ?: Thread.currentThread().contextClassLoader.getResourceAsStream("/$PKG/$name")
            ?: throw IllegalArgumentException("Resource not found: $name")
        return stream.use(block)
    }
}
