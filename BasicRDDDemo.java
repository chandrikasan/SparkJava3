package com.journaldev.sparkdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.*;

public class BasicRDDDemo {

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setMaster("yarn").setAppName("Sankir");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<Integer> numRDD1 = sparkContext.parallelize(Arrays.asList(1,2,3,4,5,6,7,8),2);

        JavaRDD<String> fileRDD = sparkContext.textFile("/user/chandrika/input.txt");

        JavaRDD<Integer> numRDD2 = sparkContext.parallelize(Arrays.asList(5,3,4,2,7,8,1,5));

        System.out.println(numRDD1.union(numRDD2).collect());
        System.out.println(numRDD1.intersection(numRDD2).collect());

        System.out.println("numRDD1:");
        System.out.println(numRDD1.collect());

        numRDD2 = numRDD1.map(new Function<Integer, Integer>() {

            private static final long serialVersionUID = -8621428343434855461L;

            @Override
            public Integer call(Integer i) throws Exception {
                return i * i * i;
            }
        });

        System.out.println("numRDD2:");
        System.out.println(numRDD2.collect());
        JavaRDD<Integer> numRDD3 = numRDD1.filter(new Function<Integer, Boolean>() {
            private static final long serialVersionUID = -3999587103085806047L;

            @Override
            public Boolean call(Integer x) throws Exception {
                return (x % 2 == 0);
            }
        });


        System.out.println("numRDD3:");
        System.out.println(numRDD3.collect());

        Integer sum = numRDD1.reduce(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1716569911400115422L;

            @Override
            public Integer call(Integer a, Integer b) throws Exception {
                return a + b;
            }
        });

        System.out.println("Elements in numRDD1: " + numRDD1.collect());

        System.out.println("Sum of the elements in numRDD1: " + sum);

        System.out.println("numRDD1.sample(false,0.5,1) : ");
        System.out.println(numRDD1.sample(false,0.5,1).collect());
        System.out.println("numRDD1.sample(true,0.5,1) : ");
        System.out.println(numRDD1.sample(true,0.5,1).collect());
        System.out.println(numRDD1.takeSample(false,4,1));
        System.out.println(numRDD1.takeSample(true,4,1));
        int result = numRDD1.fold(10, new Function2<Integer, Integer, Integer>() {
            private static final long serialVersionUID = 8092542545958591209L;

            @Override
            public Integer call(Integer i, Integer j) throws Exception {
                //System.out.println("i: " + i + " j: " + j);
                return i+j;
            }
        });
        System.out.println("Result: " + result);


        JavaRDD<String> rdd1 = sparkContext.parallelize(Arrays.asList("Good Morning", "Hello World", "hi", "Hello India", "Karnataka India"));

        JavaRDD<String> wordsRDD1 = rdd1.flatMap(new FlatMapFunction<String, String>() {

            private static final long serialVersionUID = 9032030071554039784L;
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] words = s.split(" ");
                List<String> list1 = new ArrayList<>();
                for (String str : words) {
                    list1.add(str);
                }
                return list1.iterator();
            }
        });

        System.out.println("wordsRDD1:");
        System.out.println(wordsRDD1.collect());
        System.out.println("Output of take(3) : " + wordsRDD1.take(3));
        System.out.println("Output of top(3): " + wordsRDD1.top(3));
        System.out.println("Count by value : " + wordsRDD1.countByValue());
        System.out.println("wordsRDD1.distinct() : " + wordsRDD1.distinct().collect());
        System.out.println(wordsRDD1.takeOrdered(5));



        JavaRDD<String> wordsRDD2 = rdd1.map(new Function<String, String>() {

            private static final long serialVersionUID = 8773008026862554014L;

            @Override
            public String call(String s) throws Exception {
                return ("Word: " + s);
            }
        });
        System.out.println("wordsRDD2:");
        System.out.println(wordsRDD2.collect());
        System.out.println(wordsRDD2.first());
        System.out.println(wordsRDD2.count());

        JavaRDD<String> rdd2 = sparkContext.parallelize((Arrays.asList("Hello World","Hello India")));
        System.out.println("Intersection of rdd1 and rdd2: ");
        JavaRDD<String> rdd3 = rdd1.intersection(rdd2);
        System.out.println(rdd3.collect());
        System.out.println("rdd1.subtract(rdd2):");
        System.out.println(rdd1.subtract(rdd2).collect());

        System.out.println("rdd1.cartesian(rdd2):");
        System.out.println(rdd1.cartesian(rdd2).collect());

        rdd1 = sparkContext.parallelize(Arrays.asList("Bangalore", "Karnataka", "India"));
        rdd2 = sparkContext.parallelize(Arrays.asList("Bangalore","Mumbai","Delhi"));

        System.out.println(rdd1.union(rdd2).collect());




    }
}
