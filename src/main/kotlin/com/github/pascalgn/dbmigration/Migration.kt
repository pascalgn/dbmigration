package com.github.pascalgn.dbmigration

import org.slf4j.LoggerFactory
import java.io.File
import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class Migration(val context: Context) : Runnable {
    companion object {
        val logger = LoggerFactory.getLogger(Migration::class.java)!!
    }

    override fun run() {
        val exportDir = File(context.root, "export")
        if (!exportDir.isDirectory && !exportDir.mkdir()) {
            throw IllegalStateException("Not a directory: $exportDir")
        }

        exportData(exportDir, context.input)
        importData(exportDir, context.output)
    }

    private fun exportData(outputDir: File, jdbc: Jdbc) {
        val tables = ConcurrentLinkedQueue<Table>()
        DriverManager.getConnection(jdbc.url, jdbc.username, jdbc.password).use { connection ->
            val tableNames = mutableListOf<String>()
            connection.metaData.getTables(null, jdbc.schema, "%", null).use { rs ->
                while (rs.next()) {
                    tableNames.add(rs.getString("TABLE_NAME"))
                }
            }
            tableNames.mapTo(tables) { Table(it, rowCount(connection, it)) }
        }

        if (tables.isEmpty()) {
            throw IllegalStateException("No tables found!")
        }

        logger.info("Exporting {} tables...", tables.size)

        val threads = context.inputThreads
        val executorService = Executors.newFixedThreadPool(threads);
        for (i in 1..threads) {
            executorService.execute(Exporter(outputDir, jdbc, tables))
        }
        executorService.shutdown()
        executorService.awaitTermination(14, TimeUnit.DAYS)
    }

    private fun importData(inputDir: File, jdbc: Jdbc) {
        val files = ConcurrentLinkedQueue<File>()
        inputDir.listFiles().forEach { files.add(it) }

        logger.info("Importing {} files...", files.size)

        val threads = context.outputThreads
        val executorService = Executors.newFixedThreadPool(threads);
        for (i in 1..threads) {
            executorService.execute(Importer(jdbc, files))
        }
        executorService.shutdown()
        executorService.awaitTermination(14, TimeUnit.DAYS)
    }

    private fun rowCount(connection: Connection, tableName: String): Long {
        connection.createStatement().use { statement ->
            val sql = "SELECT COUNT(1) FROM \"$tableName\""
            statement.executeQuery(sql).use { rs ->
                if (rs.next()) {
                    val count: Long = rs.getLong(1);
                    if (rs.next()) {
                        throw IllegalStateException("Expected only one row: $sql")
                    }
                    return count
                } else {
                    throw IllegalStateException("No results: $sql")
                }
            }
        }
    }
}
