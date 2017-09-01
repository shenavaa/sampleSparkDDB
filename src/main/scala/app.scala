import org.apache.hadoop.io.Text;
import org.apache.hadoop.dynamodb.DynamoDBItemWritable
import org.apache.hadoop.dynamodb.read.DynamoDBInputFormat
import org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.io.LongWritable
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SimpleApp {
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("Spark SQL basic example").enableHiveSupport().getOrCreate()
    val sc=spark.sparkContext

    var jobConf = new JobConf(sc.hadoopConfiguration)

    jobConf.set("dynamodb.input.tableName", "test")
    jobConf.set("dynamodb.output.tableName", "testOut")

    //jobConf.set("dynamodb.column.mapping","name:name,value:value")
    //jobConf.set("columns","name,value")
    //jobConf.set("columnscolumns.types","string,string")
    //jobConf.set("hive.io.filter.text","name == 'item1'")

    jobConf.set("mapred.output.format.class", "org.apache.hadoop.dynamodb.write.DynamoDBOutputFormat")
    jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.DynamoDBInputFormat")
    //jobConf.set("mapred.input.format.class", "org.apache.hadoop.dynamodb.read.HiveDynamoDBInputFormat")

    var names = sc.hadoopRDD(jobConf, classOf[DynamoDBInputFormat], classOf[Text], classOf[DynamoDBItemWritable])
    names.count()
    names.map(s => s._2).saveAsTextFile("/user/data04")
    names.map(s => s._2.getItem.get("name")).saveAsTextFile("/user/data05")
    var res = spark.sql("select * from default.ddbtable");
    res.toJavaRDD.saveAsTextFile("/user/data03")
    spark.close()
 }
}
