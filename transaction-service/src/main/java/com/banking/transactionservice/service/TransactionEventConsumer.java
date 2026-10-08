package com.banking.transactionservice.service;

import java.util.Map;

import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.banking.transactionservice.repo.TransactionRepo;

import jakarta.transaction.Transaction;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j 
@RequiredArgsConstructor 
public class TransactionEventConsumer {


    private final TransactionRepo transactionRepo;

    
    //Consume
    //verification.required
    //generate otp and ask for verification to user
    public void consumeVerificationRequired(
        @Payload Map<String, Object> payload
    ){
        try{
            String transactionId = (String) payload.get("transactionId");
            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String reason = (String) payload.get("reason");

            log.info("Received verification.required event for transactionId: {} reason: {}", transactionId, reason);

            Transaction transaction = transactionRepo.findById(transactionId).orElse("Transaction not found :" +transactionId);


        }catch(Exception e){
            //log.error("Error while consuming verification.required event: {}", e.getMessage());
        }
    }
}
