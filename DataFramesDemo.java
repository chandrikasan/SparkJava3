import org.apache.spark.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.functions.*;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.types.DataTypes.*;

public class DataFramesDemo {

    public static void main(String args[]) throws AnalysisException {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<String> RDD1 = sc.textFile("C:\\student_old.csv");

        System.out.println("Printing RDD1: ");
        System.out.println(RDD1.collect());


       JavaRDD<Row> rowRDD = RDD1.map(new Function<String, Row>() {
           @Override
           public Row call(String s) throws Exception {
               String[] words = s.split(",");
               return RowFactory.create(Integer.parseInt(words[0]),words[1],words[2],words[3],Integer.parseInt(words[4]));

           }
       });


        System.out.println("RDD of row : " + rowRDD.collect());

        StructType schema = createStructType(new StructField[]{
                createStructField("id", IntegerType, true),
                createStructField("name", StringType, true),
                createStructField("gender", StringType, true),
                createStructField("subject", StringType, true),
                createStructField("marks", IntegerType, true),

        });

        Dataset<Row> df = sqlContext.createDataFrame(rowRDD,schema);

        df.show();



        //Add a column and a default value for the column
       df.withColumn("city",functions.lit("Bangalore")).show();


        //Rename column
       df.withColumnRenamed("name","studentname").show();


        //Drop a column
        Dataset<Row> df2 = df.drop("subject");
        df2.show();


        df2.write().mode("overwrite").save("C:\\Users\\Chandrika Sanjay\\student_partitioned");
        df2.write().mode("overwrite").csv("C:\\Users\\Chandrika Sanjay\\SaveDF");

        System.out.println("First row from DF: ");
        System.out.println(df.first());

        df.select("name","marks").show();

        df.orderBy("name").show();

        df.groupBy("subject").max("marks").show();

        df.groupBy("gender").avg("marks").show();

        df.distinct().show();

        df.filter("subject == 'maths'").show();

        df.filter("marks > 90").show();

        df.sort("name").show();
        df.where("id == 3").show();
        df.where("subject == 'maths'").show();
        df.orderBy(functions.col("marks").desc()).show(5);

        System.out.println("Count: " + df.count());

        System.out.println("Min marks : ");
        df.select(functions.min("marks")).show();
        System.out.println("Max marks : ");
        df.select(functions.max("marks")).show();
        System.out.println("Sum: ");
        df.select(functions.sum("marks")).show();
        System.out.println("Avg: ");
        df.select(functions.avg("marks")).show();


        df.createTempView("student");

        Dataset<Row> result = sqlContext.sql("select max(marks) from student");

        result.show();

        df.createGlobalTempView("student1");

        Dataset<Row> result1 = sqlContext.sql("select * from global_temp.student1 where marks in (select max(marks) from global_temp.student1 group by gender)");

        result1.show();
    }
}