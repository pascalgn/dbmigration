package com.github.pascalgn.dbmigration

import java.io.File

internal interface ImportController {
    operator fun get(key: File): Boolean

    operator fun set(key: File, value: Boolean)
}
