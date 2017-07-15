package com.github.pascalgn.dbmigration

data class Target(val threads: Int, val deleteBeforeImport: Boolean,
                  val before: List<String>, val after: List<String>,
                  val jdbc: Jdbc)
