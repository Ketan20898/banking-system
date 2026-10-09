package com.banking.fraudpaymentservice.entity;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity 
@Data 
@NoArgsConstructor 
@AllArgsConstructor  
@Table (name = "payments")
public class Payment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private String id;

    private String razorpayOrderId;
    private String razorpayPaymentId;
    

    @Column (nullable = false)
    private String accountNumber;
    private String paymentFailureReason;
    @Column (nullable = false, precision = 15, scale = 2)
    private BigDecimal amount;

    @Column  ( nullable = false)
    private  String currency;

    @Enumerated (EnumType.STRING)
    private PaymentStatus status;

    private String description;
    @CreationTimestamp 
    private LocalDateTime createdAt;
    @UpdateTimestamp 
    private LocalDateTime updatedAt;
    
}
