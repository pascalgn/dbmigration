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

import org.apache.commons.lang.time.DurationFormatUtils
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics
import org.slf4j.LoggerFactory
import java.text.DecimalFormat
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

internal class Executor(private val threads: Int) {
    companion object {
        val logger = LoggerFactory.getLogger(Executor::class.java)!!
    }

    private val executorService = Executors.newFixedThreadPool(threads)
    private val progressFormat = DecimalFormat("0.00")
    private val speedFormat = DecimalFormat("0.0")

    fun execute(tasks: List<Task>) {
        logger.debug("Initializing tasks")

        tasks.forEach { executorService.submit { logErrors { it.initialize() } } }

        outer@ while (true) {
            val failed = tasks.filter { it.failed }
            if (!failed.isEmpty()) {
                executorService.shutdownNow()
                throw IllegalStateException("Some tasks failed to initialize!")
            }

            val initialized = tasks.filter { it.initialized }.count()
            logger.info("{}/{} tasks initialized", initialized, tasks.size)
            if (initialized == tasks.size) {
                break@outer
            }
            Thread.sleep(1000)
        }

        logger.debug("Executing tasks")

        val size = tasks.map { it.size }.sum()

        // stores the speed in rows per millisecond
        val statistics = DescriptiveStatistics(20)
        var previous = Info(System.currentTimeMillis(), 0, 0L)

        tasks.forEach { executorService.submit { logErrors { it.execute() } } }

        outer@ while (true) {
            val complete = tasks.filter { it.complete }.count()
            val failed = tasks.filter { it.error != null }.count()

            if (logger.isInfoEnabled) {
                val completed = tasks.map { it.completed }.sum()
                val executing = tasks.filter { it.executing }.count()
                val progress = if (size == 0L) 1.0 else completed.toDouble() / size

                if (executing != previous.executing) {
                    // the speeds are not comparable if the number of running threads changes
                    statistics.clear()
                }

                val now = System.currentTimeMillis()
                if (now != previous.time) {
                    statistics.addValue((completed - previous.completed).toDouble() / (now - previous.time))
                }

                previous = Info(now, executing, completed)

                val speed = statistics.mean
                val eta = if (speed > 0) Math.round((size - completed).toDouble() / speed) else -1

                val str = StringBuilder()
                str.append(progressFormat.format(100 * progress)).append("% (")
                str.append(complete + failed).append("/").append(tasks.size).append(")")

                if (executing > 0) {
                    str.append(", ").append(executing).append(" running")
                }

                if (failed > 0) {
                    str.append(", ").append(failed).append(" failed")
                }

                if (speed > 0) {
                    str.append(", ").append(speedFormat.format(1000 * speed)).append(" rows/s")
                }

                if (eta >= 0) {
                    str.append(", ").append(DurationFormatUtils.formatDuration(eta, "H:mm:ss"))
                }

                logger.info(str.toString())
            }

            if (complete + failed == tasks.size) {
                break@outer
            }

            Thread.sleep(1000)
        }

        executorService.shutdownNow()
    }

    private data class Info(val time: Long, val executing: Int, val completed: Long)

    private inline fun <T> logErrors(block: () -> T): T {
        try {
            return block()
        } catch (t: Throwable) {
            logger.error("Error executing task!", t)
            throw t
        }
    }
}
