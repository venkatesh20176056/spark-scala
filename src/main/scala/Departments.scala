import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object Departments {

  def departmentDataFrame(sqlContext:SQLContext) = {
    val departmentSchema = getDepartmentSchema();
    val df = sqlContext.read.format("csv")
      .option("header", "true").schema(departmentSchema)
      .load("Departments.csv")
    df;
  }

  def getDepartmentSchema() = {
    StructType(Array(
      StructField("Id", StringType, true),
      StructField("Name", StringType, true),
      StructField("ManagerID", LongType, true)
    ));
  }
}
