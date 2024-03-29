import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Job {
//  To Disable the Loggers print on the console.
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setAppName("SparkJoins").setMaster("local");
    val sparkContext = new SparkContext(sparkConf);
    val sqlContext = new SQLContext(sparkContext);
    
    val df1 = Employees.employeesDataFrame(sqlContext);
    val df2 = Departments.departmentDataFrame(sqlContext);

    df1.printSchema();
    df2.printSchema();
    df1.select("*").show();

    val df3 = df2.join(df1, df1.col("Id") === df2.col("ManagerID"));
    val df4 = df3.select(df2.col("Id").alias("DeptID"), df2.col("Name").alias("DeptName"),
      df1.col("Id").alias("ManagerId"),df1.col("Name"));
    // df4.write.parquet("1.parquet");

    df3.select(df2.col("Id").alias("DeptID"), df2.col("Name").alias("DeptName"),
      df1.col("Id").alias("ManagerId"),df1.col("Name")).show();

    df1.filter(df1("Salary") >= 20000).show();
    sparkContext.stop();
  }
}
