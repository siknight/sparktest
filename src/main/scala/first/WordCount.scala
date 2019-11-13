package first

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 打jar包方式
  * 我测试的结果是一行一读。
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf=new SparkConf().setAppName("WC");
    //创建SparkContext，该对象是提交Spark App的入口。
    val sc=new SparkContext(conf);

    //使用sc创建RDD并执行相应的transformation和action
    sc.textFile(args(0)).
      flatMap(_.split(" ")).
        map((_,1)).reduceByKey(_+_,1).
          sortBy(_._2,false).
            saveAsTextFile(args(1));

    sc.stop();

  }
}
