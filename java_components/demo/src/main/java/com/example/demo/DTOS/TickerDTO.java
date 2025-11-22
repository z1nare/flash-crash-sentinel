package com.example.demo.DTOS;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TickerDTO {
    @NotBlank
    private String event_type;
    @NotBlank
    private String timestamp;
    @NotBlank
    private String ticker;
    @NotNull
    private Double open;
    @NotNull
    private Double high;
    @NotNull
    private Double low;
    @NotNull
    private Double close;
    @Min(0)
    @NotNull
    private Integer volume;
}
