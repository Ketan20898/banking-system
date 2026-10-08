package com.banking.fraudnotificationservice.service;

import java.util.Map;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j 
public class NotificationService {
    
    @KafkaListener (topics="transaction.otp.generated")
    public void consumeOtpGenerated(
        @Payload  Map<String,Object> payload 
    ){
        try{
            String accountNumber = (String) payload.get("accountNumber");
            String transactionId = (String) payload.get("transactionId");
            String otp = (String) payload.get("otp");
            String amount = payload.get("amount").toString();
            String reason = payload.get("reason").toString();

            sendAlert(accountNumber,
                "Transaction_Verification_Required",
                String.format("Suspicious activity detected " +
                    "Reason : %s " +
                    "A Transaction of %s pending for verification "+
                    "Your otp is: %s valid for 5 minutes "+
                    "Ignore is not initiated by you and inform bank"
                 )
            );

        }catch(Exception e){
            log.error("Error in sending otp notification", e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String string, String format) {
       
    }
}
