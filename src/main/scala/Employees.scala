import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.SQLContext

object Employees {

  def employeesDataFrame(sqlContext:SQLContext) = {
    val employeeSchema = getEmployeeSchema();
    val df = sqlContext.read.format("csv")
      .option("header", "true").schema(employeeSchema)
      .load("/home/venkateshakula/Desktop/Data/Employees.csv");
    df;
  }

  def getEmployeeSchema()= {
    StructType(Array(
      StructField("Id", LongType, true),
      StructField("Name", StringType, true),
      StructField("Age", IntegerType, true),
      StructField("Salary", LongType, true)
    ));
  }
}
