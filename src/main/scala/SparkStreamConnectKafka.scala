import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @description
 * @author dick <18668485565@163.com>
 * @version V1.0.0
 * @date created on 2021/5/6
 */
object SparkStreamConnectKafka {

  def testKafkaMessage(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("LogAnalysis")
    val sc = new SparkContext(sparkConf)
    // 输入参数配置
    val Array(zkQuorum, group, topics, numThreads, outputHdfsPath, configFile, printOrWriteFile) = Array(args(0), args(1),
      args(2), args(3), args(4), args(5), args(6))
    // Spark Stream初始化
    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    // val logs = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
  }
}
