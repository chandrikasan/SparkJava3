import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;

public class SharedVariables {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        LongAccumulator acc = jsc.sc().longAccumulator();   // Initialise the accumulator.

        ArrayList<String> a1 = new ArrayList<>();

        a1.add("Ecommerce");
        a1.add("Fitness");
        final Broadcast<ArrayList<String>> bc = jsc.broadcast(a1);


        //final Broadcast<Integer> bc = jsc.broadcast(5);
        System.out.println("Broadcast variable: " + bc.value());



        JavaRDD<String> RDD1 = jsc.textFile("C:\\Users\\Chandrika Sanjay\\ExampleForCombineByKey2.txt");
        System.out.println(RDD1.collect());
        JavaPairRDD<String,Integer> RDD2 = RDD1.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] Words = s.split("\t");
                String category = Words[0];
                Integer clicks = Integer.parseInt(Words[1]);
                if (category.startsWith("Ecommerce"))
                //if ( category.matches(".*alt.*"))
                    acc.add(1);
                return new Tuple2<>(category, clicks);
            }
        });


        JavaPairRDD<String,Integer> RDD3 = RDD2.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> t) throws Exception {
                return bc.value().contains(t._1);
            }
        });



        System.out.println("Accumulator value before collect(): " + acc.value());
        System.out.println(RDD3.collect());
        System.out.println("Accumulator value after collect(): " + acc.value());
        //System.out.println("No of elements in RDD: " + RDD3.count());


    }
}
