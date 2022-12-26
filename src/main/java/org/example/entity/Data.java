package org.example.entity;


import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;

import java.util.List;

@lombok.Data
@AllArgsConstructor
@NoArgsConstructor
public class Data {
    String id;
    String title;
    List<String> keywords;
    List<String> authors;
    List<String> universeties;
    String date;
    String journal;
    String doi;
    String abstractText;
    String issn;
    String hIndex;
    List<Quartile> quartiles;
}
