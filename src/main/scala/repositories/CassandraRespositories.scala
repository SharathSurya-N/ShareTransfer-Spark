package repositories

import org.apache.spark.sql.{SparkSession, Dataset, SaveMode, Encoder}
import org.apache.spark.sql.Encoders
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

object CassandraRepository {
  
  /**
   * Generic function to write any Dataset[T] to Cassandra.
   * @param spark The SparkSession instance.
   * @param data The dataset to write.
   * @param keySpaceName The keyspace name in Cassandra.
   * @param tableName The table name in Cassandra.
   */
  def writeToCassandra[T: Encoder](spark: SparkSession, data: Dataset[T], keySpaceName: String, tableName: String): Unit = {
    
        data.write
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", keySpaceName)
        .option("table", tableName)
        .mode(SaveMode.Append)
        .save()
    
   

    println(s"Data successfully written to $keySpaceName.$tableName!")
  }

  /**
   * Generic function to read data from Cassandra into a Dataset[T].
   * @param spark The SparkSession instance.
   * @param keyspace The keyspace name in Cassandra.
   * @param table The table name in Cassandra.
   * @tparam T The case class representing the data schema.
   * @return Dataset[T].
   */
  def readFromCassandra[T: Encoder](spark: SparkSession, keyspace: String, table: String): Dataset[T] = {
    import spark.implicits._
  
        val dataDS = spark.read
        .format("org.apache.spark.sql.cassandra")
        .option("keyspace", keyspace)
        .option("table", table)
        .load()
        .as[T]
      println(s"Data successfully read from $keyspace.$table ")
      dataDS
    
    
  }
}
