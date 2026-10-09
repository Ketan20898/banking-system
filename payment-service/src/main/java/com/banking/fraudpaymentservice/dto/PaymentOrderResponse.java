package com.banking.fraudpaymentservice.dto;

import java.math.BigDecimal;

import com.banking.fraudpaymentservice.entity.PaymentStatus;

import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data 
@AllArgsConstructor 
@NotBlank 
public class PaymentOrderResponse {
    
    private String paymentId;
    private String razorpayOrderId;
    private BigDecimal amount;
    private String currency;
    private String status;
    private String razorpayKeyId;


}
