import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class HashPartitionerDemo {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("Sankir");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        JavaPairRDD<Integer,Integer> rdd1 = jsc.parallelizePairs(Arrays.asList(new Tuple2<Integer,Integer>(1,10),
                new Tuple2<>(2, 20),
                new Tuple2<>(1, 20),
                new Tuple2<>(3, 30),
                new Tuple2<>(4, 40),
                new Tuple2<>(5, 50),
                new Tuple2<>(6, 60),
                new Tuple2<>(7, 70),
                new Tuple2<>(8, 80),
                new Tuple2<>(9, 90),
                new Tuple2<>(10, 100)),4);


        System.out.println("Output of rdd1");
        System.out.println(rdd1.collect());
        System.out.println("Partitions (Before): " + rdd1.partitions().size());

        String s = new String("SANKIRTECHNOLOGIES");
        System.out.println("Hashcode: " + s.hashCode() + " Partition: " + s.hashCode() % 3 );

        String t = new String("One");
        Integer i = 17;


        System.out.println("Hashcode of " + t + " :" + t.hashCode() );
        System.out.println("Hashcode of " + i +  " :" + i.hashCode());
        System.out.println();


        HashPartitioner hp = new HashPartitioner(3);

        JavaPairRDD<Integer,Integer> partRDD = rdd1.partitionBy(hp);
        System.out.println("Partitions(After partitioning): " + partRDD.partitions().size());
        System.out.println(partRDD.collect());

        System.out.println(partRDD.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Integer, Integer>>, Integer>() {
            @Override
            public Iterator<Integer> call(Iterator<Tuple2<Integer, Integer>> t) throws Exception {
                Integer sum=0;
               Integer max=0;
                ArrayList<Integer> a1 = new ArrayList<>();
                while ( t.hasNext()) {
                    Integer currVal = t.next()._1;
                    if ( currVal > max ) {
                        max = currVal;
                    }
                }
                a1.add(max);
                return a1.iterator();
            }
        }).collect());


        System.out.println("Repartition: ");
        JavaPairRDD<Integer,Integer> repartRDD1 = partRDD.repartition(5);
        System.out.println("After repartitioning: " +  repartRDD1.partitions().size());
        System.out.println(repartRDD1.collect());

        JavaRDD<String> mapPartRDD = repartRDD1.mapPartitionsWithIndex((ind, tupleIter) -> {
            List<String> list=new ArrayList<>();
            while(tupleIter.hasNext()){
                Tuple2 element  = tupleIter.next();
                list.add("Partition number: "+ind+", key: "+ element._1() + " value: " + element._2());
            }
            return list.iterator();
        },true);
        mapPartRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });


        System.out.println("Repartition and sort within partition: ");
        JavaPairRDD<Integer,Integer> repartRDD = partRDD.repartitionAndSortWithinPartitions(new HashPartitioner(5));
        System.out.println(repartRDD.collect());

        JavaRDD<String> repartRDDwithIndex = repartRDD.mapPartitionsWithIndex((index, tupleIterator) -> {
            List<String> list=new ArrayList<>();
            while(tupleIterator.hasNext()){
                Tuple2 element1  = tupleIterator.next();
                list.add("Partition number: "+index+",key: "+ element1._1()+ " value : " + element1._2());
            }
            return list.iterator();
        }, true);

        repartRDDwithIndex.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }
}
