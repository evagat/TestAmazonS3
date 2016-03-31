/**
  * Created by egarcia on 29/03/16.
  */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.crossdata._

object testS3XData  {
  def main(args: Array[String]): Unit = {
    //Spark Context
    val conf = new SparkConf().setMaster("local[2]").setAppName("TestS3")
    val sc = new SparkContext(conf)

    //S3 Authentication
    val AccessKey = "<AccessKey>"
    val SecretAccessKey = "<SecretAccessKey>"

    //S3 Data
    val bucket = "<bucket>"
    val file = "<file>"

    //Temporary Table Data for Creation
    val nameTable = "<nameTable>"
    val atributesTable = "<atributesTable>"

    //Hadoop Configuration
    sc.hadoopConfiguration.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    sc.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", AccessKey )
    sc.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", SecretAccessKey)

    //XData Context
    val scXData = new XDContext(sc)

    // S3 Treatment
    scXData.sql("CREATE TEMPORARY TABLE "+nameTable+atributesTable+" USING com.databricks.spark.csv OPTIONS (path 's3n://"+AccessKey+":"+SecretAccessKey+"@"+bucket+"/"+file+"')")
    scXData.sql("SELECT * FROM "+nameTable).show()

  }
}