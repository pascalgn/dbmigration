package com.github.pascalgn.dbmigration

data class Import(val threads: Int, val deleteBeforeImport: Boolean, val jdbc: Jdbc)
