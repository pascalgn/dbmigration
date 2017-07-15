package com.github.pascalgn.dbmigration

import java.io.File

data class Context(val root: File, val inputThreads: Int, val input: Jdbc)
