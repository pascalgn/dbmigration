package com.github.pascalgn.dbmigration

import org.slf4j.LoggerFactory
import java.io.File
import java.net.URL
import java.net.URLClassLoader
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

        addClasspath()
        loadDrivers()

        exportData(exportDir)
        importData(exportDir)
    }

    private fun addClasspath() {
        if (context.classpath.isEmpty()) {
            return
        }
        if (javaClass.classLoader !is URLClassLoader) {
            throw IllegalStateException("Not an instance of URLClassLoader: ${javaClass.classLoader}")
        }
        val addURL = URLClassLoader::class.java.getDeclaredMethod("addURL", URL::class.java)
        addURL.isAccessible = true
        for (classpath in context.classpath) {
            val file = File(classpath)
            if (!file.exists()) {
                throw IllegalArgumentException("File not found: $classpath")
            }
            val url = file.toURI().toURL()
            addURL.invoke(javaClass.classLoader, url)
            logger.info("Added classpath entry: {}", url)
        }
    }

    private fun loadDrivers() {
        for (driver in context.drivers) {
            Class.forName(driver)
            logger.info("Loaded driver: {}", driver)
        }
    }

    private fun exportData(outputDir: File) {
        val jdbc = context.export.jdbc
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

        val threads = context.export.threads
        val executorService = Executors.newFixedThreadPool(threads);
        for (i in 1..threads) {
            executorService.execute(Exporter(outputDir, jdbc, tables))
        }
        executorService.shutdown()
        executorService.awaitTermination(14, TimeUnit.DAYS)
    }

    private fun importData(inputDir: File) {
        if (context.import.deleteBeforeImport) {
            logger.warn("Deleting existing rows before importing")
        }

        val files = ConcurrentLinkedQueue<File>()
        inputDir.listFiles().forEach { files.add(it) }

        logger.info("Importing {} files...", files.size)

        val threads = context.import.threads
        val executorService = Executors.newFixedThreadPool(threads);
        for (i in 1..threads) {
            executorService.execute(Importer(context.import.jdbc, context.import.deleteBeforeImport, files))
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
