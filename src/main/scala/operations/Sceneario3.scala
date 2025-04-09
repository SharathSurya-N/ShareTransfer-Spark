package operations

import dataSources._
import operations.InputValidator.checkForInt
import scala.annotation.tailrec
import scala.io.StdIn
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import operations.InputValidator.validateTransaction
import repositories.CassandraRepository.writeToCassandra
import repositories.CassandraRepository.readFromCassandra
import dataSources.Constants.keyspace
import dataSources.Constants.transferTable
import _root_.dataSources.Constants.sharesTableName

object Sceneario3 {

  /**
   * Method to get user input for multiple transactions.
   * @param shareDetails Dataset containing existing share details
   * @param transactionHistory Dataset containing past share transfer transactions
   * @return Seq[Transfer] the transaction to be performed
   */
  def getInput(shareDetails: Dataset[Shares], transactionHistory: Dataset[Transfer]): Seq[Transfer] = {
    print("Enter the number of transactions to perform: ")
    val count = checkForInt

    @tailrec
    def getter(count: Int, acc: Seq[Transfer]): Seq[Transfer] = {
      if (count == 0) acc
      else {
        // Read user input for sender, receiver, and percentage of shares to transfer
        print("Enter the sender name: ")
        val senderName = StdIn.readLine().trim
        print("Enter the receiver name: ")
        val receiverName = StdIn.readLine().trim
        print("Enter the percentage to share: ")
        val percentageToShare = checkForInt()

        // Validate the transaction before proceeding
        validateTransaction(senderName, receiverName, shareDetails, transactionHistory) match {
          case Some(error) =>
            println(error)
            getter(count, acc) // Retry without decrementing count

          case None =>
            getter(count - 1, Transfer(senderName, receiverName, percentageToShare) +: acc)
        }
      }
    }

    getter(count, Seq())
  }

  /**
   * Method to process multiple transactions in a batch.
  * @param spark SparkSession instance
   * @param shareDetails Dataset containing existing share details
   * @param transactionHistory Dataset containing past share transfer transactions
   */
  def multipleTransaction(spark: SparkSession, shareDetails: Dataset[Shares], transferDetails: Dataset[Transfer]): Unit = {
    import spark.implicits._

    // Collect transactions from user input
    val transactionToDone = getInput(shareDetails, transferDetails)
    val transactions = transactionToDone.toDS()
    
    //Sender has enough shares before processing
    val validTransfers = transactions.groupBy("share_tranferor").agg(sum("percentage_of_share").alias("sum"))
    .where("sum<100")
    validTransfers.show()
    if (validTransfers.isEmpty) {
      println("A transfer exceeds the sender's available shares!")
      return
    }
    
    // Process transactions recursively
    helper(transactionToDone.length - 1, spark, shareDetails, transactionToDone)
    
    // Write completed transactions to Cassandra
    writeToCassandra(spark, transactions, keyspace, transferTable)
    println("Transactions Processed Successfully!")
  }

  /**
   * Function to process share transfers one by one.
  * @param spark SparkSession instance
   * @param shareDetails Dataset containing existing share details
   * @param transferDetails Seq[Transfer] hold the transaction details
   */
  @tailrec
  def helper(count: Int, spark: SparkSession, shareDetails: Dataset[Shares], transferDetails: Seq[Transfer]): Unit = {
    import spark.implicits._

    if (count < 0) {
      // Group by sender to calculate total percentage transferred
      val transfer = transferDetails.toDS()
        .groupBy("share_tranferor")
        .agg(sum("percentage_of_share").alias("total_percentage"))
      
      // Compute updated sender share values
      val updatedSenderDF = shareDetails
        .join(transfer, $"share_holder" === $"share_tranferor")
        .withColumn("reduction", ($"total_percentage" / 100) * $"share_owned")
        .withColumn("updated_share", $"share_owned" - $"reduction")
        .drop("total_percentage", "share_tranferor", "reduction", "share_owned")
        .withColumn("share_owned", $"updated_share".cast("int")) // Ensure correct data type
        .drop("updated_share")
        .as[Shares]

      
      // Write updated share ownership back to Cassandra
      writeToCassandra(spark, updatedSenderDF, keyspace, sharesTableName)
    } else {
      // Process each transfer transaction
      val senderName = transferDetails(count).share_tranferor
      val receiverName = transferDetails(count).share_reciever
      val percentageToShare = transferDetails(count).percentage_of_share
      
      // Fetch sender details and compute transferred shares
      val senderShares = shareDetails.filter(_.share_holder == senderName).cache()
      val transferredShares = senderShares.flatMap { row =>
        val shareName = row.share_name
        val senderShareData = row.share_owned
        val transferAmount = (senderShareData * percentageToShare / 100).toInt

        // Deduct from sender and add to receiver
        Seq(Shares(shareName, receiverName, transferAmount))
      }.as[Shares].cache()
      
      // Fetch receiver's existing shares
      val receiverShares = shareDetails.filter(_.share_holder == receiverName).cache()
      
      // Merge sender and receiver shares, then aggregate
      val updatedShares = transferredShares.union(receiverShares)
        .groupBy("share_name", "share_holder")
        .agg(sum("share_owned").cast("int").alias("share_owned"))
        .as[Shares]
      
      updatedShares.show()
      
      // Write updated data back to Cassandra
      writeToCassandra(spark, updatedShares, keyspace, sharesTableName)
      
      // Recursive call with updated data for next transaction
      helper(count - 1, spark, readFromCassandra[Shares](spark, keyspace, sharesTableName), transferDetails)
    }
  }
}
