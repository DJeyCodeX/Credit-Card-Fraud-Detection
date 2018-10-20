import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import sqlContext.implicits._

val ssc = new StreamingContext(sc,Seconds(10))

sc.setLogLevel("ERROR")
val kvs = KafkaUtils.createStream(ssc,"localhost:2181", "spark-streaming-consumer", Map("DJE"-> 1))

val lines = kvs.map(_._2)
val data = lines.map(line => line.split(",")) 
val hiveObj = new HiveContext(sc)

case class creditData(Merchant_id:String, Average_Amount_transaction_day:String, Transaction_amount:String, Is_declined:String, Total_Number_of_declines_day:String, isForeignTransaction:String, isHighRiskCountry:String, Daily_chargeback_avg_amt:String, six_month_avg_chbk_amt:String, six_month_chbk_freq:String, isFradulent:String)

data.foreachRDD({ rdd =>
  import sqlContext.implicits._
  val credit_s = rdd.map(row => creditData(row(0), row(1), row(2), row(3), row(4), row(5), row(6), row(7), row(8), row(9), row(10))).toDF()
credit_s.write.saveAsTable("default.creditcard")
 })

 ssc.start()