/**
 * @description
 * @author dick <18668485565@163.com>
 * @version V1.0.0
 * @date      created on 2021/5/6
 *
 *
 *            下载相应版本的winutils.exe（不同版本请参考链接：https://github.com/cdarlint/winutils，我下载的是2.7.3），然后将其解压到任意目录，这里建议放在Hadoop的bin目录下。
 *
 *            法①：然后在系统环境中，添加一个新的系统变量，命名为：HADOOP_HOME，路径为你所解压的路径，配置变量的时候，不用加bin目录，因为spark会自动读取到HADOOP_HOME,然后加上bin。（建议用这一种）
 *
 *            法②：或者在代码中添加一行代码：（这里我是下载了hadoop-common-2.2.0-bin-master文件，里面包含winutils.exe）
 *
 *            System.setProperty("hadoop.home.dir", "E:\\Hadoop\\hadoop-common-2.2.0-bin-master")
 */

import org.apache.spark.{SparkConf, SparkContext}


object WordCount {
  def main(args: Array[String]): Unit = {
    //System.setProperty("hadoop.home.dir", "E:\\Hadoop\\hadoop-common-2.2.0-bin-master")
    val config = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(config)
    val lines = sc.textFile("in")
    val words = lines.flatMap(_.split(" "))
    val wordToOne = words.map((_, 1))
    val wordToSum = wordToOne.reduceByKey(_ + _)
    val result = wordToSum.collect()
    result.foreach(println)

    //第一种方法
  }

}