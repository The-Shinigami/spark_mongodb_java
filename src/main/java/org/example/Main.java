package org.example;

import org.apache.spark.sql.*;


import static org.apache.spark.sql.functions.*;

public class Main {
    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("TesSpark").config("spark.driver.bindAddress", "127.0.0.1").config("spark.master", "local").config("spark.mongodb.input.uri","mongodb://localhost:27017/test_db.data").config("spark.mongodb.output.uri","mongodb://localhost:27017/new_data.data").config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/test_db.data")
                .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/new_data.data").master("local")
                .getOrCreate();


        readData(spark);

    }

    public static void readData(SparkSession spark){
        Dataset<Row> df = spark.read().format("mongodb").load();
        df.printSchema();
        System.out.println("Total : "+df.count());
        process(df);
    }

    public static void process(Dataset<Row> data){
           Dataset<Row> tranformedF=data.select(col("title"),col("journal"),col("issn"),col("hIndex"),col("quartiles"),col("date"),col("universeties"),explode(col("authors")).as("author"));
        Dataset<Row> transformedDF1=tranformedF.select(col("title"),col("journal"),col("issn"),col("hIndex"),col("quartiles"),col("date"),col("author"),explode(col("universeties")).as("university"));
        Column university=functions.split(column("university"), ",");

//        Map<String, Column> mymap = new HashMap<String, Column>();
//        int [] sizes=new int[4964];
//        final int[] i = {0};
//        Dataset<String> namesDS =
//                transformedDF1.select(col("university")).map(
//                        new MapFunction<Row, String>() {
//                            public String call(Row row) {
//                                int l=row.getString(0).split(",").length;
//                                sizes[i[0]]=l;
//                                i[0]++;
//                                return row.getString(0);
//
//                            }
//
//                        },
//                        Encoders.STRING());
//
//        namesDS.show();

//       final int[] sizes=new int[4964];
//        final int[] i = {0};
//        Map<String, Column> mymap = new HashMap<String, Column>();
//        transformedDF1.select(size(functions.split(column("university"), ",")).alias("keys"));
//        .foreach(key ->{
//
//                sizes[i[0]]=key.getInt(0);
//
//                i[0]++;
//                    mymap.put("city", university.getItem(key.getInt(0) - 3));
//                    mymap.put("country", university.getItem(key.getInt(0)- 2));
//                    mymap.put("lab", university.getItem(0));
//                    mymap.put("unv", university.getItem(1));



//                }
//
//         );

//        ArrayList<Column> city=new ArrayList<>();
//        ArrayList<Column> country=new ArrayList<>();
//        ArrayList<Column> unv=new ArrayList<>();
//        ArrayList<Column> lab=new ArrayList<>();
//        for(int [] j={0};j[0]<sizes.length;j[0]++){
//
////            city.add(university.getItem(sizes[j[0]]-2));
////
////            country.add(university.getItem(sizes[j[0]]-1));
////
////            unv.add(university.getItem(sizes[j[0]]-3));
////
////            lab.add(university.getItem(0));
////            mymap.put("city",university.getItem(sizes[j[0]]-3));
////            mymap.put("country",university.getItem(sizes[j[0]]-2));
//            mymap.put("city",university.getItem(2));
//            mymap.put("country",university.getItem(3));
//            mymap.put("lab",university.getItem(0));
//            mymap.put("unv",university.getItem(1));
//
//
////            test.withField("city",university
////                    .getItem(sizes[j[0]]-2));
////            test.withField("country",university
////                    .getItem(sizes[j[0]]-1));
////            test.withField("lab",university
////                    .getItem(0));
////            test.withField("university",university
////                    .getItem(sizes[j[0]]-3));
//
//
//
//        }
//        mymap.put("city",city);
//        mymap.put("country",country);
//        mymap.put("lab",lab);
//        mymap.put("unv",unv);

//        Dataset<Row> transformedDF2=transformedDF1
//                .withColumn("city",mymap.get("city"))
//                .withColumn("country",mymap.get("country"))
//                .withColumn("laboratory", mymap.get("lab"))
//                .withColumn("university", mymap.get("unv"))
//                ;

        Dataset<Row> transformedDF2=transformedDF1
                .withColumn("city",university.getItem(2))
                .withColumn("country",university.getItem(3))
                .withColumn("laboratory", university.getItem(0))
                .withColumn("university", university.getItem(1))
                ;

        transformedDF2=transformedDF2.withColumn("year", functions.split(col("date")," ").getItem(2)).drop(col("date"));
        transformedDF2.show(100);

        //To write data in mongodb
   //     writeData(transformedDF2);


    }

    //To write data in mongodb
//    public static void writeData(Dataset<Row> data){
//        data.write().format("mongodb").option("uri", "mongodb://127.0.0.1").option("database", "new_data").option("collection", "data").mode("overwrite").save();
//
//    }

}