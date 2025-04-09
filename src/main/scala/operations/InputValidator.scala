package operations

import scala.io.StdIn
import org.apache.spark.sql.Dataset
import dataSources._
import dataSources.Constants.maxPercentage

object InputValidator{
    /**
     * Function to check whether the input is in int
     * @return Integer entered
    */
    def checkForInt():Int={
        try {
            val input=StdIn.readInt()
            input
        } catch {
            case ex: Exception =>{
                print("Enter a valid format :")
                checkForInt()
            } 
        }
    }
     /**
       * Function to check the entered percentage is below 100 else throws error
       * @param percentage The eneter percenatge
       *
       * @throws Error if the percentage is higher than 100
       */
    def validatePercentage(percentage: Int): Unit = {
        if (percentage > maxPercentage) throw new IllegalStateException("Percentage should be lesser than or equal to 100")
    }
     /** Validates the share transfer transaction by checking all conditions.
   *
   * @param senderName The name of the sender
   * @param receiverName The name of the receiver
   * @param shareDetails Dataset containing existing share details
   * @param transactionHistory Dataset containing past share transactions
   * @return Option[String] - None if validation is successful, Some(errorMessage) if validation fails
   */
  def validateTransaction(
      senderName: String,
      receiverName: String,
      shareDetails: Dataset[Shares],
      transactionHistory: Dataset[Transfer]
  ): Option[String] = {
    if(senderName==receiverName){
      return Some("A share cannot be transfer to themself")
    }
    // Check if sender has already received shares in past transactions
    val senderHistory = transactionHistory.filter(_.share_reciever == senderName)
    val receiverHistory = transactionHistory.filter(_.share_tranferor == receiverName)
    //If the senderHistory is empty states that the sender is an reciver in another transaction
    if (!senderHistory.isEmpty) {
      return Some(s"$senderName is a reciever in a transaction cannot be a sender in another transaction")
    }
    //If the receiverHistory is not empty then the reciver is a sender in some transaction
    if (!receiverHistory.isEmpty) {
      return Some(s"$receiverName is a sender in a transaction cannot be a receiver in another transaction")
    }

    // Check if sender exists and holds shares
    val senderShares = shareDetails.filter(_.share_holder == senderName)
    if (senderShares.isEmpty) {
      return Some(s"$senderName does not hold any shares.")
    }

    

    // If all checks pass, return None (indicating no errors)
    None
  }
}