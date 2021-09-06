package excerciseTest;

import minsait.ttaa.datio.engine.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.Common.INPUT_PATH;

import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.junit.Assert.*;

public class SparkTest {

    private static Dataset<Row> df;
    private static SparkSession sparkSession = null;
//    private Transformer transformer = new Transformer();

    @Before
    public void beforeFunction(){
        //spark = SessionSpark.getSparkSession()
        sparkSession = SparkSession.builder()
                .master(SPARK_MODE)
                .getOrCreate();

        df = sparkSession.read().option(HEADER, true).option(INFER_SCHEMA, true).csv(INPUT_PATH);

        System.out.println("Before Function");
    }

    @After
    public void afterFunction() {
        if (sparkSession != null) {
            sparkSession.stop();
        }
//        sparkSession.stop();
        System.out.println("After Function");
    }

    @Test
    public void methodFourTest() {
        df = df.filter(
            rank_by_nationality_position.column().$less(3)
                    .or( (catByAge.column().equalTo("A").and(potencialVsOverall.column().$greater(1.25))) )
                    .or( ((catByAge.column().equalTo("B").or(catByAge.column().equalTo("C"))).and(potencialVsOverall.column().$greater(1.15))) )
                    .or( catByAge.column().equalTo("D").and(rank_by_nationality_position.column().$less(5)) )
            );

    }
}
