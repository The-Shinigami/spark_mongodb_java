package org.example.service;



import org.apache.spark.sql.Row;
import org.example.entity.Data;
import org.example.entity.NewData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;


public interface DataService {
    public List<NewData> getData();
    public HashMap getCountAndPercentageByYear(String column, String year);
    public List<Map<String, Object>> getCountByMonthAndYear();

    public Map<String, Object> getCountByCountryAndYear();

    public Map getCountQuartileByYear();
}
