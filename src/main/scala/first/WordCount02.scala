package first

import org.apache.spark.{SparkConf, SparkContext}

/**
  * 本地调试
  */
object WordCount02 {
  def main(args: Array[String]): Unit = {
    //创建SparkConf并设置App名称
    val conf=new SparkConf().setAppName("WC").setMaster("local[*]");
    //创建SparkContext，该对象是提交Spark App的入口。
    val sc=new SparkContext(conf);

    //使用sc创建RDD并执行相应的transformation和action
    sc.textFile("C:\\Users\\lisi\\Downloads\\fruit.tsv").
      flatMap(_.split(" ")).
      map((_,1)).reduceByKey(_+_,1).
      sortBy(_._2,false).
      saveAsTextFile("C:\\Users\\lisi\\Downloads\\out04");
    sc.stop();
  }
}
