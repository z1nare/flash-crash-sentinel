package com.example.demo.DTOS;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TickerDTO {
    private String event_type;
    private String timestamp;
    private String ticker;
    private Double open;
    private Double high;
    private Double low;
    private Double close;
    private Integer volume;
}
