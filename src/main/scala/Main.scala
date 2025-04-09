import operations._
import utils.SparkConnection.createSparkSession
import dataSources.Constants.shareData
import dataSources.Transfer
import repositories.CassandraRepository._
import org.apache.spark.sql.functions._
import dataSources.Constants._
import com.datastax.spark.connector.cql.CassandraConnector
import scala.io.StdIn
import dataSources.Shares
import operations.InputValidator.checkForInt
object example extends App{
    //Creates the spark session
    val spark=createSparkSession()
    import spark.implicits._
    //Every time the program isexecuted the data in the table truncated 
    //so that for each time the program runs with new values
    val connector = CassandraConnector(spark.sparkContext)
    connector.withSessionDo { session =>
        session.execute(s"TRUNCATE $keyspace.$sharesTableName")
        session.execute(s"TRUNCATE $keyspace.${transferTable}")
    }
    //Updating the data of the shareData which is described
    writeToCassandra(spark,shareData.toDS(),keyspace,sharesTableName)
    //Menu which will take the scenario as an inout and call the respective function
    def menuDisplay():Unit=
        {   //The share details is readed from the database
            val shares=readFromCassandra[Shares](spark,keyspace,sharesTableName).cache()
            shares.show()
            //The transaction history is readed from the database
            val transfer=readFromCassandra[Transfer](spark,keyspace,transferTable).cache()
            transfer.show()
            //Displays the available operation that an user can select to perform
            println("1  Scenario 1 to transfer shares to an new user")
            println("2  Scenario 2 to transfer the shares to an existing user")
            println("3  Scenario 3 for multiple transfer of the shares")
            println("4  Exit")
            print("Enter a scenario to transfer the shares :  ")
            //Gets the input as int and the checkForInt is validated so that the numberFormat exception is handled
            val input=checkForInt
            input match {
                //If the input ismatched with 1 then the scenario one function is called
                case 1=>Scenario1.addNewShareHolder(spark,shares,transfer)
                //If the input is matched with the 2 then the scenario 2 function is called
                case 2=>Scenario2.transferShareToExistUser(spark,shares,transfer)
                // If the input matched with the 3 then then scenario 3 is called
                case 3=>Sceneario3.multipleTransaction(spark,shares,transfer)
                //if input is 4 then it is exited
                case 4=>{
                    shares.unpersist()
                    transfer.unpersist()
                    return
                }
                //Default input
                case _: Int => println("Enter a valid option")
            }
            shares.unpersist()
            transfer.unpersist()
            //Recurcively calls untill it is exited
            menuDisplay()
        }
        //Function call of the menuDisplay
        menuDisplay  
    }
