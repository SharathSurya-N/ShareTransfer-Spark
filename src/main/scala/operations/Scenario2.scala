package operations

import org.apache.spark.sql.{SparkSession, Dataset}
import dataSources._
import scala.io.StdIn
import org.apache.spark.sql.functions._
import operations.InputValidator._
import repositories.CassandraRepository.writeToCassandra
import dataSources.Constants._

object Scenario2 {
  /**Function to share the shares to existing user
   *
   * @param spark SparkSession instance
   * @param shareDetails Dataset containing existing share details
   * @param transactionHistory Dataset containing past share transfer transactions
   */
  def transferShareToExistUser(
      spark: SparkSession,
      shareDetails: Dataset[Shares],
      transactionHistory: Dataset[Transfer]
  ): Unit = {
    import spark.implicits._

    // Input collection from user
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
      case None =>{}
    }
    val receiverValidator = shareDetails.filter(_.share_holder == receiverName)
    if (receiverValidator.isEmpty) {
       println("A share can only be transferred to an existing user in this scenario.")
       return
    }
    val senderShares = shareDetails.filter(_.share_holder == senderName).cache()

    // Compute new share distribution after transfer
    val transferredShares = senderShares.flatMap { row =>
      val shareName = row.share_name
      val senderShareData = row.share_owned
      val transferAmount = (senderShareData * percentageToShare / 100).toInt

      Seq(
        Shares(shareName, senderName, senderShareData - transferAmount),  // Deduct from sender
        Shares(shareName, receiverName, transferAmount)                  // Add to receiver
      )
    }.as[Shares].cache()
    
    val receiverShares = shareDetails.filter(_.share_holder == receiverName).cache()
    
    // Merge and aggregate shares for final updated dataset
    val updatedShares = transferredShares.union(receiverShares)
      .groupBy("share_name", "share_holder")
      .agg(sum("share_owned").cast("int").alias("share_owned"))
      .as[Shares].cache()
    
    // Write updated share details to Cassandra
    writeToCassandra(spark, updatedShares, keyspace, sharesTableName)

    // Write transaction history for the transfer
    writeToCassandra(
      spark,
      Seq(Transfer(senderName, receiverName, percentageToShare)).toDS(),
      keyspace,
      transferTable
    )
    updatedShares.show()
    senderShares.unpersist()
    updatedShares.unpersist()
    receiverShares.unpersist()
    transferredShares.unpersist()
  }
}
