package minsait.ttaa.datio;

import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import static minsait.ttaa.datio.common.Common.SPARK_MODE;

public class Runner {

    static SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();

    public static void main(String[] args) {
        Transformer engine = new Transformer(spark);

//        try (JavaSparkContext context = new JavaSparkContext(spark.sparkContext())) {
//
//            List<Integer> integers = Arrays.asList(1,2,3,4,5,6,7,8);
//
//            JavaRDD<Integer> javaRDD = context.parallelize(integers,2); // Crea 3 ejecuciones paralelas
//
//            javaRDD.foreach((VoidFunction<Integer>) integer ->
//                    System.out.println("JavaRDD" + integer));
//
//            Thread.sleep(1000000);
//            context.stop();
//
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }
}
