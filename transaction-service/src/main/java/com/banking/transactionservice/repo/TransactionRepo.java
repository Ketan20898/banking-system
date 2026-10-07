package com.banking.transactionservice.repo;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.banking.transactionservice.entity.Transaction;

@Repository 
public interface TransactionRepo extends JpaRepository<Transaction, String> {
    
    List<Transaction> findBySenderAccountNumberOrderByCreatedAtDesc(String accountNumber);
    
}
