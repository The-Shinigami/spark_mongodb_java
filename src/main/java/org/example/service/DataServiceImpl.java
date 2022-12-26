package org.example.service;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF0;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.example.entity.CountryValues;
import org.example.entity.NewData;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Encoders;
import scala.collection.immutable.Seq;

import static org.apache.spark.sql.functions.*;


public class DataServiceImpl implements DataService{

    public SparkSession spark;
    public List<NewData> newDataList;
    public Dataset<Row> newDataRow;
    public DataServiceImpl(){
        spark = SparkSession.builder().appName("TesSpark").config("spark.driver.bindAddress", "127.0.0.1").config("spark.master", "local").config("spark.mongodb.input.uri","mongodb://localhost:27017/test_db.data").config("spark.mongodb.output.uri","mongodb://localhost:27017/new_data.data").config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/test_db.data")
                .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/new_data.data").master("local")
                .getOrCreate();

        getData();
    }

    @Override
    public List<NewData>  getData(){
        Dataset<Row> df = spark.read().format("mongodb").load();

//        df.printSchema();
        return getNewData(df);
    }
    public List<NewData> getNewData(Dataset data){

        Dataset<Row> tranformedF=data.select(col("title"),col("journal"),col("issn"),col("hIndex"),col("quartiles"),col("date"),col("universeties"),col("authors"));

        Column date = functions.split(column("date")," ");


        Dataset<Row> transformedDF2 = tranformedF.withColumn("yearAndMonth", functions.split(col("date")," "));


//        MapFunction<String, String> removeComma = (String s) -> s.replace(",", "");
//        transformedDF2 = transformedDF2.withColumn("yearAndMonth", map(transformedDF2.col("yearAndMonth"), callUDF(removeComma.toString())));



        transformedDF2  = transformedDF2.select(col("title"),col("journal"),col("issn"),col("hIndex"),col("quartiles"),col("date"),col("universeties"),col("authors"),col("yearAndMonth"), functions.expr("yearAndMonth[size(yearAndMonth) - 1]").as("year"));
        transformedDF2  = transformedDF2.select(col("title"),col("journal"),col("issn"),col("hIndex"),col("quartiles"),col("date"),col("universeties"),col("authors"),col("yearAndMonth"),col("year"), functions.expr("yearAndMonth[size(yearAndMonth) - 2]").as("month")).drop("date","yearAndMonth");


        UDF1<String, String> removeComma = (String s) -> s.replace(".", "");
        spark.udf().register("removeCommaUDF", removeComma, DataTypes.StringType);
        transformedDF2  = transformedDF2 .withColumn("year", functions.callUDF("removeCommaUDF", transformedDF2.col("year")));


        newDataRow = transformedDF2;
        Dataset<NewData> ds = transformedDF2.as(Encoders.bean(NewData.class));
        List<NewData> newData = ds.takeAsList((int) transformedDF2.count());
        newDataList = newData;

        return  newData;

}

   @Override
    public HashMap getCountAndPercentageByYear(String column,String year){

       Dataset<Row> rowByYear = newDataRow.filter(functions.col("year").like( year));
       Dataset<Row> rowByPrevYear = newDataRow.filter(functions.col("year").like(Integer.toString(( Integer.parseInt(year)-1))));

       long countYearL = 1,countPrevYearL = 1;
       float percentageL = 0;

       switch (column){
           case "journal" :

               countYearL = rowByYear.count();
               countPrevYearL = rowByPrevYear.count();

               break;
           case "author":

               Dataset<Row> dfExploded = rowByYear.withColumn("author", functions.explode(rowByYear.col("authors")));
               Dataset<Row> dfDistinct = dfExploded.select("author").distinct();
               countYearL = dfDistinct.count();

               Dataset<Row> dfExplodedPrev= rowByPrevYear.withColumn("author", functions.explode(rowByPrevYear.col("authors")));
               Dataset<Row> dfDistinctPrev = dfExplodedPrev.select("author").distinct();
               countPrevYearL = dfDistinctPrev.count();
               break;

           case "country" :

               Dataset<Row> dfExplodedC = rowByYear.withColumn("university", functions.explode(rowByYear.col("universeties")));
               Dataset<Row> dfSplitC = dfExplodedC.withColumn("split", functions.split(dfExplodedC.col("university"), "\\s*,\\s*"));
               dfSplitC.cache();
               Dataset<Row> dfLastC = dfSplitC.select( col("split"),functions.expr("split[size(split) - 1]").as("last")).drop("split");
               dfLastC = dfLastC.distinct();
               countYearL = dfLastC.count();

               Dataset<Row> dfExplodedC2 = rowByPrevYear.withColumn("university", functions.explode(rowByPrevYear.col("universeties")));
               Dataset<Row> dfSplitC2 = dfExplodedC2.withColumn("split", functions.split(dfExplodedC2.col("university"), "\\s*,\\s*"));
               dfSplitC2.cache();
               Dataset<Row> dfLastC2 = dfSplitC2.select( col("split"),functions.expr("split[size(split) - 1]").as("last")).drop("split");
               dfLastC2 = dfLastC2.distinct();
               countPrevYearL = dfLastC2.count();
               break;

           case "university":
               Dataset<Row> dfExplodedU = rowByYear.withColumn("university", functions.explode(rowByYear.col("universeties")));
               Dataset<Row> dfSplitU = dfExplodedU.withColumn("split", functions.split(dfExplodedU.col("university"), "\\s*,\\s*"));
               Column filteredArray = functions.filter(dfSplitU.col("split"), s -> s.contains("University"));
               Column firstElement = functions.element_at(filteredArray, 1);
               Dataset<Row> dfSelectedU = dfSplitU.select(firstElement.as("filtered_element"));
               Dataset<Row> dfWithoutNulls = dfSelectedU.filter(col("filtered_element").isNotNull());
               countYearL = dfWithoutNulls.distinct().count();

               Dataset<Row> dfExplodedU2 = rowByPrevYear.withColumn("university", functions.explode(rowByPrevYear.col("universeties")));
               Dataset<Row> dfSplitU2 = dfExplodedU2.withColumn("split", functions.split(dfExplodedU2.col("university"), "\\s*,\\s*"));
               Column filteredArray2 = functions.filter(dfSplitU2.col("split"), s -> s.contains("University"));
               Column firstElement2 = functions.element_at(filteredArray2, 1);
               Dataset<Row> dfSelectedU2 = dfSplitU2.select(firstElement2.as("filtered_element"));
               Dataset<Row> dfWithoutNulls2 = dfSelectedU2.filter(col("filtered_element").isNotNull());
               countPrevYearL = dfWithoutNulls2.distinct().count();

               break;

       }

       percentageL = (countYearL * 100)/(countYearL + countPrevYearL);
       String percentageS = "";
       if(percentageL >= 0)
           percentageS = "+" + Float.toString(percentageL);
       else
           percentageS = Float.toString(percentageL);

       HashMap map = new HashMap<>();
       map.put("countL", countYearL);
       map.put("percentageL",percentageS);

        return map;
    }

    @Override
    public List<Map<String, Object>> getCountByMonthAndYear() {

        Dataset<Row> dfGrouped = newDataRow.groupBy(col("year"),col("month")).count();
        dfGrouped = dfGrouped.orderBy(functions.col("year").desc(), functions.col("month"));
        List<Row> results = dfGrouped.collectAsList();

        List<Map<String, Object>> list = new ArrayList<>();

        for (Row row : results) {
            // Get the year and month as strings
            String year = row.getString(0);
            String month = row.getString(1);

            // Check if the year is already in the list of hashmaps
            Map<String, Object> yearMap = list.stream().filter(map -> map.get("key").equals(year)).findFirst().orElse(null);

            // If the year is not in the list of hashmaps, create a new hashmap for it
            if (yearMap == null) {
                yearMap = new HashMap<>();
                yearMap.put("key", year);
                yearMap.put("value", new HashMap<>());
                list.add(yearMap);
            }

            // Get the value hashmap
            Map<String, Object> valueMap = (Map<String, Object>) yearMap.get("value");

            // Get the labels and data lists from the value hashmap
            List<String> labels = (List<String>) valueMap.get("labels");
            List<Integer> data = (List<Integer>) valueMap.get("data");

            // If the lists are null, create new ones
            if (labels == null) {
                labels = new ArrayList<>();
                valueMap.put("labels", labels);
            }
            if (data == null) {
                data = new ArrayList<>();
                valueMap.put("data", data);
            }

            // Add the month and count to the labels and data lists
            labels.add(month);
            data.add((int) row.getLong(2));
        }
        for (Map<String, Object> map : list) {
            // Get the year and value hashmap
            String year = (String) map.get("key");
            Map<String, Object> valueMap = (Map<String, Object>) map.get("value");

            // Set the title in the value hashmap
            valueMap.put("title", "Journals for " + year);
        }


        return list;
    }

    @Override
    public Map<String, Object> getCountByCountryAndYear(){
        Dataset<Row> dfExplodedC = newDataRow.withColumn("university", functions.explode(newDataRow.col("universeties")));
        Dataset<Row> dfSplitC = dfExplodedC.withColumn("split", functions.split(dfExplodedC.col("university"), "\\s*,\\s*"));
        Dataset<Row> dfLastC = dfSplitC.select(col("year"),col("split"), functions.expr("split[size(split) - 1]").as("country")).drop("split");

        dfLastC = dfLastC.withColumn("country", functions.when(
                        dfLastC.col("country").equalTo("U.K")
                                .or(dfLastC.col("country").equalTo("U.K."))
                                .or(dfLastC.col("country").equalTo("UK"))
                                .or(dfLastC.col("country").equalTo("United kingdom")), "United Kingdom")
              .when(
                        dfLastC.col("country").equalTo("USA")
                                .or(dfLastC.col("country").equalTo("United States"))
                                .or(dfLastC.col("country").equalTo("The United States of America")), "USA")
               .when(
                        dfLastC.col("country").equalTo("CN")
                                .or(dfLastC.col("country").equalTo("China"))
                                .or(dfLastC.col("country").equalTo("china"))
                        , "China")
                .when(
                        dfLastC.col("country").equalTo("France")
                                .or(dfLastC.col("country").equalTo("France."))
                        , "France")
                .when(
                        dfLastC.col("country").equalTo("P.R.China")
                                .or(dfLastC.col("country").equalTo("PR China"))
                                .or(dfLastC.col("country").equalTo("P. R. China"))
                        , "France")
                .when(
                        dfLastC.col("country").equalTo("UAE")
                                .or(dfLastC.col("country").equalTo("United Arab Emirates"))
                        , "UAE")
                .when(
                        dfLastC.col("country").contains("Lab")
                                .or(dfLastC.col("country").contains("company"))
                                .or(dfLastC.col("country").contains("University"))
                                .or(dfLastC.col("country").contains("??"))
                                .or(dfLastC.col("country").equalTo(""))
                        , "Unknown")
                .when(
                        dfLastC.col("country").contains("Hong Kong")

                        , "Hong Kong")
                .otherwise(dfLastC.col("country")));


                 dfLastC.cache();


// Group by year and count the number of universities
        Dataset<Row> dfGrouped = dfLastC.groupBy("year","country").count().orderBy("year");

// Pivot the data to get the desired result
        Dataset<Row> dfPivoted = dfGrouped.groupBy("country" ).pivot("year").df();

   dfPivoted.cache();

        List<String> years = dfPivoted.select("year").distinct().orderBy("year").as(Encoders.STRING()).collectAsList();
        List<String> countries = dfPivoted.select("country").distinct().orderBy("country").as(Encoders.STRING()).collectAsList();


        Map<String, Map<String, Long>> yearToCountryToCount = new LinkedHashMap<>();
        for (String year : years) {
            Map<String, Long> countryToCount = new LinkedHashMap<>();
            for (String country : countries) {
                Long value ;
                Column yearFilter = dfPivoted.col("year").equalTo(year);
                Column countryFilter = dfPivoted.col("country").equalTo(country);
                Dataset<Row> rows = dfPivoted.filter(yearFilter.and(countryFilter));
                 rows.cache();
                if(rows.count()>0) {
                Row row = rows.first();

                     value = row != null ? row.getLong(2) : 0;
                }else{
                    value = (long)0;
                }
                countryToCount.put(country, value);
            }
            yearToCountryToCount.put(year, countryToCount);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("labels", years);
        List<Map<String, Object>> values = new ArrayList<>();
        for (String country : countries) {
            Map<String, Object> value = new LinkedHashMap<>();
            value.put("label", country);
            List<Long> data = new ArrayList<>();
            for (String year : years) {
                Map<String, Long> countryToCount = yearToCountryToCount.get(year);
                data.add(countryToCount.get(country));
            }
            value.put("data", data);
            values.add(value);
        }
        result.put("labels",years);
        result.put("values",values);
        return  result;
    }

    @Override
    public Map getCountQuartileByYear() {
        Dataset<Row> dfQuartiles = newDataRow.withColumn("quartile", explode(newDataRow.col("quartiles"))).select(col("quartile"),col("quartile").getItem("year").as("yearQ")).where("yearQ = '2021'");
        dfQuartiles.show();
        Dataset<Row> dfCounts = dfQuartiles.select(col("quartile").getItem("quartile").as("quartile")).groupBy("quartile").count();
        List<Map<String, String>> quartileData = dfCounts.toJavaRDD()
                .map(row -> {
                    Map<String, String> map = new HashMap<>();
                    map.put("quartile", row.getAs("quartile").toString());
                    map.put("count", row.getAs("count").toString());
                    return map;
                }).collect();
        Map result = new LinkedHashMap<>();
        result.put("labels", quartileData.stream().map(m -> m.get("quartile")).collect(Collectors.toList()));
        result.put("data", quartileData.stream().map(m -> m.get("count")).collect(Collectors.toList()));
        result.put("title", "Quartiles percentage in 2021");
        return result ;
    }
}