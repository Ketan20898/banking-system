package com.banking.transactionservice.entity;

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
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@AllArgsConstructor 
@NoArgsConstructor 
@Data 
public class Transaction {
    
    @Id 
    @GeneratedValue (strategy =GenerationType.IDENTITY )
    private String id;  
    @Column (nullable =false)
    private String senderAccountNumber;
    @Column (nullable =false)
    private String receiverAccountNumber;
    @Column (nullable =false, precision = 15, scale = 2)
    private BigDecimal amount;

    @Column (nullable = false)
    @Enumerated(EnumType.STRING)
    private TransactionStatus status;

    @Column (nullable = false)
    @Enumerated(EnumType.STRING)
    private TransactionType type;
    
    private String description;
    private String failureReason;
    private String referenceNo;
    @CreationTimestamp 
    private LocalDateTime createdAt;
    @UpdateTimestamp
    private LocalDateTime completedAt;
}
