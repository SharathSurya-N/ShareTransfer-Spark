package dataSources

import utils.SparkConnection.createSparkSession

object Constants{
    val sharesTableName="shares"
    val keyspace="sharathsurya"
    val transferTable="transfer"
    //Data of the share details as described
    val shareData=Seq(
        Shares("Share1","Jim",1000),
        Shares("Share1","Pam",500),
        Shares("Share2","Pam",500),
        Shares("Share2","Dwight",500),
        Shares("Share3","Jim",250),
        Shares("Share3","Pam",250),
        Shares("Share3","Dwight",500)
    )
    val maxPercentage=100
}