import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.*;

public class DataFramesUsingSparkSession {
    public static void main(String[] args) throws AnalysisException {
        SparkSession sparkSession = SparkSession.builder().master("local").appName("sankir").getOrCreate();

        Dataset<Row> df1 = sparkSession.read().format("csv").option("header", "true").load("C:\\Users\\Chandrika Sanjay\\student.csv");

        df1.createTempView("student");


        sparkSession.sql("select * from student").show();

        Dataset<Row> df2 = df1.filter(new FilterFunction<Row>() {
            @Override
            public boolean call(Row row) throws Exception {
                if ( row.anyNull())
                    return false;
                else
                    return true;
            }
        });

        df2.createOrReplaceTempView("student");
        df2.show();

        Dataset<Row> df4 = sparkSession.sql("select * from student where marks > 80 ");

        df4.createOrReplaceTempView("student1");
        sparkSession.sql("select name, count(*) from student1 group by name having count(*)>=2 ").show();

        sparkSession.sql("select name, count(*) from student1 group by name having count(*)>=2 ").show();
        Dataset<Row> df3 = sparkSession.sql("select * from student where marks in (select max(marks) from student group by subject)");
        df3.show();

        df3.write().mode("overwrite").csv("C:\\highestInSubjects");

        df3.where("id == 3").show();

    }

}
