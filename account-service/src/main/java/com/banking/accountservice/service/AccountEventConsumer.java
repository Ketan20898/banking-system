package com.banking.accountservice.service;

import java.math.BigDecimal;
import java.util.Map;

import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j
@RequiredArgsConstructor
public class AccountEventConsumer {

    private final AccountService accountService;

    //consume if transaction is completed and credit the receiver account
    public void consumeTransactionCompleted(
        @Payload Map<String, Object> payload
    ){
        try{
            String receiverAccountNumber = (String) payload.get("receiverAccountNumber");
            BigDecimal amount = new BigDecimal(payload.get("amount").toString());
            log.info("Crediting to Account :{} amount : {}", receiverAccountNumber,amount);
            accountService.creditBalance(receiverAccountNumber, amount);
        }catch(Exception e){
            log.error("Error while consuming transaction completed event: {}", e.getMessage());
        }   
    }
//consume when fraud detected and block the account
    public void consumeFraudDetected(
        @Payload Map<String, Object> payload
    ){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            log.info("Fraud detected for Account :{}", accountNumber);
            accountService.blockAccount(accountNumber);
        }catch(Exception e){
            log.error("Error while consuming fraud detected event: {}", e.getMessage());
        }   
    }
}
