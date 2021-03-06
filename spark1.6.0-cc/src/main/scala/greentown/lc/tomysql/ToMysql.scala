package greentown.lc.tomysql

import java.sql.{Connection, DriverManager, PreparedStatement}


/**
  * Created by saicao on 2017/8/28.
  */
object ToMysql {
  def main(args: Array[String]): Unit = {
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "insert into blog(name, count) values (?, ?)"
    try {
      conn = DriverManager.getConnection("jdbc:mysql://183.134.74.36:3306/sparkstreamtest","root", "gt123")

        ps = conn.prepareStatement(sql)
        ps.setString(1, "hehe")
        ps.setString(2, "123")
        ps.executeUpdate()

    } catch {
      case e: Exception => println("Mysql Exception")
    } finally {
      if (ps != null) {
        ps.close()
      }
      if (conn != null) {
        conn.close()
      }
    }


  }
}
