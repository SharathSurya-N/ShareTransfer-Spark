package operations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import dataSources._
import scala.io.StdIn
import operations.InputValidator._
import repositories.CassandraRepository.writeToCassandra
import dataSources.Constants._
object Scenario1 {
    /**
     * Function to add a new user shares from an existing shareholder.
     * Validates transaction history to ensure rules are followed.
     *
     * @param spark SparkSession object
     * @param shareDetails Dataset containing existing share details
     * @param transactionHistory Dataset containing past transfer transactions
     */
    def addNewShareHolder(spark: SparkSession, shareDetails: Dataset[Shares], transactionHistory: Dataset[Transfer]): Unit = {
        import spark.implicits._

        // Collect user input
        print("Enter the sender name: ")
        val senderName = StdIn.readLine()
        print("Enter the receiver name: ")
        val receiverName = StdIn.readLine()
        print("Enter the percentage to share: ")
        val percentageToShare = checkForInt
        validatePercentage(percentageToShare)

        validateTransaction(senderName, receiverName, shareDetails, transactionHistory) match {
            case Some(errorMessage) =>
                println(errorMessage) // Print the error and stop execution
                return
            case None => {}
            }
        val receiverValidator = shareDetails.filter(_.share_holder == receiverName)
        if (!receiverValidator.isEmpty) {
            println("A share can only be transferred to an new user in this scenario.")
            return
        }   
        val senderShares = shareDetails.filter($"share_holder" === senderName).persist()
        // Compute new share distribution
        val transferredShares = senderShares.flatMap { row =>
            val shareName = row.share_name
            val senderShareData = row.share_owned
            val transferAmount = (senderShareData * percentageToShare / 100).toInt

            Seq(
                Shares(shareName, senderName, senderShareData - transferAmount),  // Updated sender's shares
                Shares(shareName, receiverName, transferAmount)                   // New receiver's shares
            )
        }.as[Shares]
        // Write updated shares to Cassandra
        writeToCassandra(spark, transferredShares, keyspace, sharesTableName)

        // Write transaction history to Cassandra
        writeToCassandra(
            spark,
            Seq(Transfer(senderName, receiverName, percentageToShare)).toDS(),
            keyspace, transferTable
        )
        senderShares.unpersist()
    }
}
