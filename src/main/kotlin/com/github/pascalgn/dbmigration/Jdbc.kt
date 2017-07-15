package com.github.pascalgn.dbmigration

data class Jdbc(val url: String, val username: String, val password: String, val schema: String, val quotes: Boolean)
