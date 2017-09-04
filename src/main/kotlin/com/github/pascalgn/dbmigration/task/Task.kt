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

internal abstract class Task {
    var size = -1L
        @Synchronized get
        protected @Synchronized set(value) {
            if (value < 0) {
                throw IllegalArgumentException("Negative size not allowed: $value")
            } else if (field == -1L) {
                field = value
            } else {
                throw IllegalStateException("Size has already been set!")
            }
        }

    var completed = 0L
        @Synchronized get
        protected @Synchronized set(value) {
            if (value < 0) {
                throw IllegalArgumentException("Negative completion not allowed: $value")
            } else if (size == -1L) {
                throw IllegalStateException("Size has not yet been initialized!")
            } else if (value > size) {
                throw IllegalArgumentException("Completion cannot be greater than size: $value > $size")
            } else if (field == size) {
                throw IllegalStateException("Completion cannot be set after the task has been completed!")
            } else {
                field = value
            }
        }

    var error: Throwable? = null
        @Synchronized get
        private @Synchronized set

    val failed get() = error != null
    val initialized @Synchronized get() = (error == null && size != -1L)
    val complete @Synchronized get() = (error == null && size != -1L && completed == size)

    var executing: Boolean = false
        @Synchronized get
        private @Synchronized set

    private val lock = object {}

    fun initialize() {
        synchronized(lock) {
            if (size != -1L) {
                throw IllegalStateException("Already initialized!")
            }
            try {
                doInitialize()
            } catch (t: Throwable) {
                error = t
                throw t
            }
            if (size < 0L) {
                throw IllegalStateException("Initialization failed: invalid size: $size")
            }
        }
    }

    /**
     * Initializes the <code>size</code> of this task. A size of 0 means that this task will not be executed.
     */
    protected abstract fun doInitialize()

    fun execute() {
        synchronized(lock) {
            if (size < 0L) {
                throw IllegalStateException("Not initialized!")
            } else if (completed != 0L) {
                throw IllegalStateException("Already executed!")
            } else if (error != null) {
                throw IllegalStateException("Cannot start a task with failed initialization!")
            } else if (completed < size) {
                try {
                    executing = true
                    doExecute()
                } catch (t: Throwable) {
                    error = t
                    throw t
                } finally {
                    executing = false
                }
                if (completed < size) {
                    throw IllegalStateException("Execution did not complete: $completed < $size")
                }
            }
        }
    }

    protected abstract fun doExecute()
}
