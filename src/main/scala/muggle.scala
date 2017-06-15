import java.sql.{DriverManager, Statement, SQLException}
import scala.collection.JavaConversions._
import org.apache.hive.jdbc.HiveStatement

/**
  * Created by Sreekanth Mahesala on 7/22/16.
  */
object muggle {

  val DEFAULT_QUERY_PROGRESS_INTERVAL = 100
  var driver, user, password, connectionString: String = ""

  def main(args: Array[String]): Unit = {

    if(args.length <= 1) println("Insufficient parameters, first config then files")

    try {
      dbConfig(args{0})
    } catch {
      case e: Exception => println("Unable to read db configurations" + e.getMessage)
      System.exit(1)
    }

    args.drop(1).foreach(start(_))
  }

  def dbConfig(settingFile: String ): Unit = {

    val str = scala.io.Source.fromFile(settingFile).mkString
    val dbSetting = xml.XML.loadString(str)

    driver = (dbSetting \ "driver").text
    user = (dbSetting \ "username").text
    password = (dbSetting \ "password").text
    connectionString = (dbSetting \ "connection_string").text

  }

  def start(file:String){

    //Import driver, define the coonnection and create a statement object
    Class.forName(driver)

    val connection = DriverManager.getConnection(connectionString,user,password)
    val stmt = connection.createStatement()



    val sqlCommands = io.Source.fromFile(file).mkString.split(";")

    for(sqlCommand <- sqlCommands if (sqlCommand.length > 1) & (!sqlCommand.replace('\n',' ').trim().startsWith("--"))) {
      hiveExecute(stmt,sqlCommand.replace('\n',' ').trim())
    }

  }

  def hiveExecute(stmt:Statement, sqlStmt: String): Unit = {

    try {
      println("Running: "+ sqlStmt)

      val logThread = new Thread(new createHiveLog(stmt))
      logThread.setDaemon(true)
      logThread.start()
      stmt.execute(sqlStmt)
      logThread.interrupt()
      logThread.join(1000)

    } catch {
      case e: SQLException => {
        println(e.getMessage)
        e.printStackTrace()
      }
      System.exit(1)
    }
  }

  //def disconnect() = connection.close()

  class createHiveLog(statement:Statement) extends Runnable {

    val hiveStmt = statement.asInstanceOf[HiveStatement]

    def run() {
        while(hiveStmt.hasMoreLogs){
          try{
              for(log <- hiveStmt.getQueryLog()){
                Console.println(log)
              }
            Thread.sleep(100)
          } catch {
            case e:SQLException => System.err.println(e.getMessage)
            case e:InterruptedException => System.err.println(e.getMessage + " Probably the job ended successfully")
          }
        }
      }

  }

}
