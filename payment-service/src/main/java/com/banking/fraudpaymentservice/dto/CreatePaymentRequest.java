package com.banking.fraudpaymentservice.dto;

import java.math.BigDecimal;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data 
@NoArgsConstructor 
@AllArgsConstructor 
public class CreatePaymentRequest {

    @NotBlank (message = "Account number required")
    private String accountNumber;
    @NotBlank (message = "Amount is required")
    @Positive (message = "amount must be positive")
    private BigDecimal amount;
    private String description;

    
}
