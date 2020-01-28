import org.apache.spark.*;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.rdd.RDD;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class RangePartitionerDemo {
    public static void main(String[] args)  {
        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);



        System.out.println("Range Partitioning:");

        JavaPairRDD<Integer, String> pairRdd = jsc.parallelizePairs(Arrays.asList(new Tuple2<>(1, "A"),
                        new Tuple2<>(2, "B"),new Tuple2<>(3, "C"),
                        new Tuple2<>(4, "D"),new Tuple2<>(5, "E"),
                        new Tuple2<>(6, "F"),new Tuple2<>(7, "G"),
                        new Tuple2<>(8, "H"),new Tuple2<>(9, "A"),
                new Tuple2<>(10,"B"),new Tuple2<>(11, "A")
               ));

        System.out.println(pairRdd.collect());


        RDD<Tuple2<Integer, String>> rdd = JavaPairRDD.toRDD(pairRdd);



        System.out.println("Before partitioning : " + pairRdd.partitions().size());


        RangePartitioner rangePartitioner = new RangePartitioner(4, rdd, false, scala.math.Ordering.Int$.MODULE$ , scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        JavaPairRDD<Integer, String> rangePartRDD = pairRdd.partitionBy(rangePartitioner);

        rangePartRDD.collect();
        System.out.println("After partitioning: " + rangePartRDD.partitions().size());

        JavaRDD<String> mapPartRDD2 = rangePartRDD.mapPartitionsWithIndex((index, tupleIterator) -> {
            List<String> list=new ArrayList<>();
            while(tupleIterator.hasNext()){
                list.add("Partition number:"+index+",key:"+tupleIterator.next()._1() + "\n");
            }
            return list.iterator();
        }, true);

        System.out.println(mapPartRDD2.collect());

        }
}