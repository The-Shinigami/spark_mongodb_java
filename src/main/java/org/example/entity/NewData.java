package org.example.entity;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Array;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NewData {
    String title;
    String journal;
    String issn;
    String hIndex;
    List<Quartile> quartiles;
    List<String> authors;
   List<String> universeties;
//    String city;
//    String country;
//    String laboratory;
    String year;
    String month;

}
