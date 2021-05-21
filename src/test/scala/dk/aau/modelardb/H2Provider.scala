package dk.aau.modelardb

import dk.aau.modelardb.arrow.SegmentGroupSchema

import java.nio.charset.StandardCharsets
import java.sql.{Connection, DriverManager, Statement}
import java.time.Instant

trait H2Provider {

  def withH2[A](fun: Statement => A): A = {
    val conn = DriverManager.getConnection("jdbc:h2:mem:")
    try {
      val statement = conn.createStatement()
      fun(statement)
    } finally {
      conn.close()
    }
  }

  def withH2AndTestData[A](fun: Statement => A): A = {
    val conn = DriverManager.getConnection("jdbc:h2:mem:")
    try {
      insertTestData(conn)
      val statement = conn.createStatement()
      fun(statement)
    } finally {
      conn.close()
    }
  }

  def insertTestData(conn: Connection) = {
    val updateStmt = conn.createStatement()
    updateStmt.executeUpdate(SegmentGroupSchema.createTableSQL)
    updateStmt.close()

    val insertStmt = conn.prepareStatement("INSERT INTO segment VALUES(?, ?, ?, ?, ?, ?)")
    val rng = scala.util.Random
    (1 to 10).foreach{ id =>
      val start = Instant.now()
      val end = start.plusSeconds(rng.nextInt(100))
      insertStmt.setInt(1, id)
      insertStmt.setLong(2, start.toEpochMilli)
      insertStmt.setLong(3, end.toEpochMilli)
      insertStmt.setInt(4, 123)
      insertStmt.setBytes(5, "params".getBytes(StandardCharsets.UTF_8))
      insertStmt.setBytes(6, "gaps".getBytes(StandardCharsets.UTF_8))
      insertStmt.addBatch()
    }
    insertStmt.executeBatch()
    insertStmt.close()
  }
}
