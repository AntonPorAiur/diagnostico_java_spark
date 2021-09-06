package minsait.ttaa.datio.engine;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.functions.*;

public class Transformer extends Writer {

    private SparkSession spark;

    public Transformer(){}

    public Transformer(@NotNull SparkSession spark) {
        this.spark = spark;
        Dataset<Row> df = readInput();

        df.printSchema();

        df = cleanData(df);
        df = ageRangeWindow(df);
        df = rankByNationalityAndTeamPosition(df);
        df = potencialVsOverall(df);
        df = customizedFilter(df);
        df = columnSelection(df);

        // for show 100 records after your transformations and show the Dataset schema
        df.show(100, false);
        df.printSchema();

        // Uncomment when you want write your final output
//        write(df);
    }

    /**
     *  Ejercicio 1
     */
    private Dataset<Row> columnSelection(Dataset<Row> df) {
        return df.select(
                shortName.column(),
                longName.column(),
                age.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                teamPosition.column(),
                catByAge.column(),
                rank_by_nationality_position.column(),
                potencialVsOverall.column()
//                catHeightByPosition.column()
        );
    }

    /**
     * @return a Dataset readed from csv file
     */
    private Dataset<Row> readInput() {
        Dataset<Row> df = spark.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(INPUT_PATH);
        return df;
    }

    /**
     * @param df
     * @return a Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    private Dataset<Row> cleanData(Dataset<Row> df) {
        df = df.filter(
                teamPosition.column().isNotNull()
                    .and(shortName.column().isNotNull())
                    .and(overall.column().isNotNull()
                )
        );

//        df.withColumn(longName.getName(),when(longName.column().contains("?"), "DESCONOCIDO                        "));

        return df;
    }

    /**
     * @param df is a Dataset with players information (must have team_position and height_cm columns)
     * @return add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    private Dataset<Row> exampleWindowFunction(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());

        Column rank = rank().over(w);

        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");

//        df = df.withColumn(catHeightByPosition.getName(), rule);

        return df;
    }


    /**
     *  Ejercicio 2
     */
    private Dataset<Row> ageRangeWindow(Dataset<Row> df) {

        df = (df.withColumn(catByAge.getName(), when(age.column().$less(23),"A" )
                .when(age.column().$less(27),"B")
                .when(age.column().$less(32),"C")
                .otherwise("D")));

        return df;
    }


    /**
     *  Ejercicio 3
     */
    private Dataset<Row> rankByNationalityAndTeamPosition(Dataset<Row> df) {
        WindowSpec w = Window
                .partitionBy(teamPosition.column(),nationality.column())
                .orderBy(overall.column().desc());

        df = df.withColumn(rank_by_nationality_position.getName(), row_number().over(w));

        return df;
    }

    /**
     *  Ejercicio 4
     */
    private Dataset<Row> potencialVsOverall(Dataset<Row> df) {

        df = (df.withColumn(potencialVsOverall.getName(), col(potential.getName()).divide(col(overall.getName()))));

        return df;
    }

    /**
     *  Ejercicio 5
     */
    private Dataset<Row> customizedFilter(Dataset<Row> df) {

        df = df.filter(
                rank_by_nationality_position.column().$less(3)
                .or( (catByAge.column().equalTo("A").and(potencialVsOverall.column().$greater(1.25))) )
                .or( ((catByAge.column().equalTo("B").or(catByAge.column().equalTo("C"))).and(potencialVsOverall.column().$greater(1.15))) )
                .or( catByAge.column().equalTo("D").and(rank_by_nationality_position.column().$less(5)) )
        );

        return df;
    }


}
