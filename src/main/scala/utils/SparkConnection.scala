package utils
import org.apache.spark.sql.SparkSession
/**
 * Utility object to create and manage SparkSession with Cassandra configuration.
 */
object SparkConnection {

  /**
   * Creates and returns a SparkSession configured for Cassandra connection.
   * @return SparkSession instance.
   */
  def createSparkSession(): SparkSession = {
    val spark = SparkSession.builder()
      .appName("Cassandra-Spark-Connector")
      .master("local[*]")
      .config("spark.cassandra.connection.host", "192.168.11.38")
      .config("spark.cassandra.auth.username", "sharathsurya")
      .config("spark.cassandra.auth.password", "March@2399")
      .config("spark.cassandra.output.consistency.level", "ONE") 
      .config("spark.cassandra.input.consistency.level", "ONE")
  
      .getOrCreate()
    println("Spark connected to Cassandra")
    spark
  }
}
