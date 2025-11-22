// NewsDTO.java
package com.example.demo.DTOS;

import com.fasterxml.jackson.annotation.JsonProperty; // CRUCIAL for actor1_code
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class NewsDTO {
    @NotBlank
    private String event_type;
    @NotBlank
    private String timestamp;
    @NotBlank
    private String ticker;
    @NotBlank
    private String headline;
    private String url;
    private String cameo_code;

    // Python uses actor1_code, Java standards prefer actor1Code (camelCase).
    // The @JsonProperty annotation explicitly tells Jackson how to map the JSON key.
    @JsonProperty("actor1_code")
    private String actor1Code;

    private Double goldstein;
}