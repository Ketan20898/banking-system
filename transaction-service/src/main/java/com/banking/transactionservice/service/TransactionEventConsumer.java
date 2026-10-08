package com.banking.transactionservice.service;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.repo.TransactionRepo;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j 
@RequiredArgsConstructor 
public class TransactionEventConsumer {


    private static final long OTP_DURATION_MINUTES = 5;

    private final TransactionRepo transactionRepo;

    private final RedisTemplate<String,String> redisTemplate;

    private final KafkaTemplate<String,Object> kafkaTemplate;

    private final TransactionService transactionService;


    private static final String TRANSACTION_OTP_GENERATED_TOPIC = "transaction.otp.generated";

    
    //Consume
    //verification.required
    //generate otp and ask for verification to user
    @KafkaListener (topics = "verification.required")
    public void consumeVerificationRequired(
        @Payload Map<String, Object> payload
    ){
        try{
            String transactionId = (String) payload.get("transactionId");
            String senderAccountNumber = (String) payload.get("senderAccountNumber");
            String reason = (String) payload.get("reason");

            log.info("Received verification.required event for transactionId: {} reason: {}", transactionId, reason);

            Transaction transaction = transactionRepo.findById(transactionId)
            .orElseThrow(() -> new RuntimeException("Transaction not found :" +transactionId));


            if(transaction.getStatus() != TransactionStatus.PROCESSING){
                log.warn("Transaction with transactionId: {} is not in PROCESSING status. Current status: {}", transactionId, transaction.getStatus());
                return;
            }   

            ///generate otp
            String otp = String.format("%06d", (int) Math.random()*900000+100000);

            //store in redis which expires in 5 mints
            String otpKey = "verification:otp"+transactionId;

            redisTemplate.opsForValue().set(otpKey, otp, Duration.ofMinutes(OTP_DURATION_MINUTES));

            transaction.setStatus(TransactionStatus.PENDING_VERIFICATION);
            transactionRepo.save(transaction);

            log.info("Otp generated for the verification :{} expires in :{} min", transactionId, OTP_DURATION_MINUTES);

            //nofify user

            Map<String , Object> otpEvent = new HashMap<>();
            otpEvent.put("TransactionId", transactionId);
            otpEvent.put("Account Number", senderAccountNumber);    
            otpEvent.put("Otp", otp);
            otpEvent.put("Reason", reason);
            otpEvent.put("Amount", payload.get("amount").toString());

            kafkaTemplate.send(TRANSACTION_OTP_GENERATED_TOPIC, otpEvent);

        }catch(Exception e){
            log.error("Error while consuming verification.required event: {}", e.getMessage());
        }
    }

    @KafkaListener (topics = "fraud.check.clean")
    public void ConsumeFraudCheckCleanResultEvent(
        @Payload  Map<String,Object> payload
    ){
        try{
            String transactionId = (String) payload.get("transactionId");

            transactionService.processCleanResult(transactionId);

        }catch(Exception e){
            log.error("Error in processing fraud check result reason : {}",e.getMessage());
        }
    }
}
