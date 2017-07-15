package com.github.pascalgn.dbmigration

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import java.io.File
import java.nio.file.Files
import java.sql.DriverManager
import java.sql.ResultSet

abstract class AbstractIT {
    protected lateinit var directory: File

    @BeforeEach
    fun before() {
        directory = Files.createTempDirectory("root").toFile()
    }

    @AfterEach
    fun after() {
        directory.deleteRecursively()
    }

    fun <T> select(url: String, sql: String, block: (ResultSet) -> T): T {
        DriverManager.getConnection(url, "", "").use { connection ->
            connection.createStatement().use { statement ->
                statement.executeQuery(sql).use { rs ->
                    return block.invoke(rs)
                }
            }
        }
    }
}
