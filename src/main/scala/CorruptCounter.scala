/**
 * Created by davidsuarez on 3/03/16.
 */
  import org.apache.spark.{SparkConf, SparkContext}

  object CorruptCounter extends App {
    val logFile = "src/main/resources/papers.csv"
    val conf = new SparkConf().setMaster("local[2]").setAppName("OlympicMedals")
    val sc = new SparkContext(conf)
    val file = sc.textFile(logFile)

    val corruptPaymnentRDD = file.map(x => {
      val arr = x.split(",")
      new CorruptPayment(arr(0), Integer.parseInt(arr(1)), Integer.parseInt(arr(2)))
    })

    corruptPaymnentRDD.map(x => (x.name, x.payment)).reduceByKey(_ + _).saveAsTextFile("/tmp/corrupt/result.txt")
  }

