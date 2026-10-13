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

    @KafkaListener (topics = "transaction.completed")
    public void consumeTransactionCompleted(
        @Payload Map<String,Object> payload
    ){
        try {
            String senderAccount =(String) payload.get("senderAccountNumber");
            String receiverAccount =(String) payload.get("receiverAccountNumber");
            String amount = payload.get("amount").toString();

            sendAlert(senderAccount,"DEBIT ALERT", 
                String.format("%s debited from account %s", amount,senderAccount)
            );

             sendAlert(receiverAccount,"CREDIT ALERT", 
                String.format("%s credit to account %s", amount,receiverAccount);
            );


        } catch (Exception e) {
            log.error("error in sending alerts ",e.getMessage());
        }
    }


    @KafkaListener (topics = "fraud.detected")
    public void consumeFraudDetected(
        @Payload  Map<String,Object> payload
    ){
        try {
            String accountNumber =(String) payload.get("accountNumber");
            String reason = payload.get("reason").toString();

            sendAlert(accountNumber, "SUSPICIOUS_ACTIVITY_DETECTED", 
                String.format("Your account %s is blocked due to suspicious activty reason is : %s ", accountNumber,reason)
            );


        } catch (Exception e) {
           log.error("error in sending fraud alerts ",e.getMessage());
        }
    }
    @KafkaListener (topics = "transaction.refunded")
    public void consumeTransactionRefunded(
        @Payload  Map<String,Object> payload
    ){
        try {
            
            String accountNumber  =(String) payload.get("senderAccountNumber");
            String amount = payload.get("amount").toString();
            String reason = payload.get("reason").toString();

            sendAlert(accountNumber, "REFUND_PROCESSED", 
                String.format("Transaction of amount %s is cancelled due to %s, and redund to account %s ", amount,reason,accountNumber)
            );


        } catch (Exception e) {
             log.error("error in sending refund alerts ",e.getMessage());
        }
    }
    @KafkaListener (topics="payment.failed")
    public void consumePaymentFailed(
        @Payload Map<String,Object> payload
    ){
        try {
              String accountNumber = (String) payload.get("");
            String amount = payload.get("amount").toString();

            sendAlert("PAYMENT_FAILED", accountNumber,
                String.format("Payment of %s cound not be processed and please try again or contact bank", amount)
            );

        } catch (Exception e) {
            log.error("Error in sending payment failure alerts ",e.getMessage());
        }
    }

    @KafkaListener (topics = "payment.completed")
    public void consumePaymentCompleted(
        @Payload  Map<String,Object> payload
    ){
        try {
            String accountNumber = (String) payload.get("");
            String amount = payload.get("amount").toString();
            String razorpayPaymentId = payload.get("razorpayPaymentId").toString();


            sendAlert("Payment_SUCCESSFUL", accountNumber, 
                String.format("payment of %s is completed ,Razorpay id is : %s ",amount,razorpayPaymentId)
            );


        } catch (Exception e) {
            log.error("Error in sending payment completed alerts ",e.getMessage());
        }
    }

    private void sendAlert(String accountNumber, String subject, String message) {
       log.info("-----------------------------");
       log.info("Account : {}",accountNumber);
       log.info("Subject : {}",subject);
       log.info("Message : {}",message);
       log.info("-----------------------------"));
    }
}
