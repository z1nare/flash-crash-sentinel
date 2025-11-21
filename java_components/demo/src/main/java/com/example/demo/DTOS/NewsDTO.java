package com.example.demo.DTOS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NewsDTO {
    private String event_type;
    private String timestamp;
    private String ticker;
    private String headline;
    private String url;
    private String cameo_code;
    private String actor1code;
    private Double goldstein;
}
