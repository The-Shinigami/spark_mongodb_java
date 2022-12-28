package org.example;


import com.google.gson.Gson;
import org.apache.spark.sql.*;
import org.example.entity.Data;
import org.example.entity.NewData;
import org.example.service.DataService;
import org.example.service.DataServiceImpl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static spark.Spark.*;

public class Main {


    public static void main(String[] args) {
        DataService dataService = new DataServiceImpl();

        before((request, response) -> {
            response.header("Access-Control-Allow-Origin", "*");
            response.header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS");
            response.header("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Requested-With");
        });


        Gson gson = new Gson();
        get("/cards", (req, res) -> {
            res.type("application/json");
            String title = req.queryParams("title");
            String year = req.queryParams("year");

            return dataService.getCountAndPercentageByYear(title,year);
        }, gson ::toJson);

        get("/bars", (req, res) -> {
            res.type("application/json");
            return dataService.getCountByMonthAndYear();
        }, gson ::toJson);

        get("/lines", (req, res) -> {
            res.type("application/json");
            return dataService.getCountByCountryAndYear();
        }, gson ::toJson);

        get("/donut", (req, res) -> {
            res.type("application/json");
            String year = req.queryParams("year");
            return dataService.getCountQuartileByYear(year);
        }, gson ::toJson);

        get("/publications", (req, res) -> {
            res.type("application/json");
            return dataService.getPublications();
        }, gson ::toJson);


    }



}