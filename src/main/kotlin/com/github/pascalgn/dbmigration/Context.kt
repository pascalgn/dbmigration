package com.github.pascalgn.dbmigration

import java.io.File

data class Context(val root: File,
                   val classpath: List<String>, val drivers: List<String>,
                   val inputThreads: Int, val input: Jdbc,
                   val outputThreads: Int, val output: Jdbc)
