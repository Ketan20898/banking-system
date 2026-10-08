package com.banking.frauddetectionservice.service;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.frauddetectionservice.client.AccountServiceClient;
import com.banking.frauddetectionservice.model.FraudCheckResult;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j 
@RequiredArgsConstructor 
public class FraudDetectionService {

    private final AccountServiceClient accountServiceClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RedisTemplate<String, Object> redisTemplate;

    @Value ("${fraud.velocity.count}")
    private int fraudVelocityCount;

    @Value ("${fraud.amount.multiplier}")
    private double fraudAmountMultiplier;

    @Value ("${fraud.max.balance.percentage}")
    private double maxBalancePercentage;

    private static final String VERIFICATION_REQUIRED_TOPIC = "verification.required";
    private static final String TRANSACTION_CLEAN_TOPIC = "fraud.check.clean";
    
    public void checkTransaction(Map<String,Object> payload) {
        // Implement fraud detection logic here
        //log.info("Checking transaction for fraud: {}", payload.get("transactionId"));
        String senderAccountNumber = (String) payload.get("senderAccountNumber");
        BigDecimal amount = new BigDecimal( payload.get("amount").toString()); 
        String transactionId = (String) payload.get("transactionId");

        //fetch RealBalance from account service via feign
        
        BigDecimal senderBalance = new BigDecimal(accountServiceClient.getAccountBalance(senderAccountNumber));

        log.info("checking transaction details for : {} account :{} amount :{} balance : {}",
        transactionId, senderAccountNumber, amount, senderBalance
        );

        FraudCheckResult result =performFraudCheck(senderAccountNumber, amount, senderBalance, transactionId);

        if(result.getIsFraudDetected()) {
            log.warn("Suspicious activity detected for transaction account no - : {} with transactionId - : {} reason :{}", senderAccountNumber, transactionId, result.getReason());
            // Handle fraud case (e.g., notify, block transaction, etc.)

            Map<String, Object> verificationEvent = new HashMap<>();
            verificationEvent.put("transactionId", transactionId);
            verificationEvent.put("AccountNumber", senderAccountNumber);
            verificationEvent.put("amount", amount);
            verificationEvent.put("reason", result.getReason());
                
            kafkaTemplate.send(VERIFICATION_REQUIRED_TOPIC,transactionId, verificationEvent);


        } else {
            log.info("Transaction {} is valid.", transactionId);
            // Proceed with the transaction
            Map<String, Object> transactionCleanEvent = new HashMap<>();
            transactionCleanEvent.put("transactionId", transactionId);
            transactionCleanEvent.put("isFraudDetected", false);
            transactionCleanEvent.put("Reason", null);
            kafkaTemplate.send(TRANSACTION_CLEAN_TOPIC, transactionId, transactionCleanEvent);    

        }



    }

    private FraudCheckResult performFraudCheck(String senderAccountNumber, BigDecimal amount, BigDecimal senderBalance,
            String transactionId) {
        
                //velocity check : 5 or more transactions in a minute

                if(isVelocityExceeded(senderAccountNumber)) {
                    return new FraudCheckResult(true, " Too many transactions in a minute");
                }

                if(amountCheck(senderAccountNumber, amount)) {
                    return new FraudCheckResult(true, "Unusual High amount than average transaction");
                }

                if(amount.compareTo(senderBalance) > 0  && isBalanceCheck(senderBalance, amount)) {
                    return new FraudCheckResult(true, "Transaction amount is more than account balance");
                }

                return new FraudCheckResult(false, "No fraud detected");
                // more than average transaction amount for the account
                // trnsaction around 90% of users account balance


    
}

    private boolean isBalanceCheck(BigDecimal senderBalance, BigDecimal amount) {
    
        BigDecimal maxAllowed = senderBalance.multiply(BigDecimal.valueOf(maxBalancePercentage));
        
        log.info("Balance Check -amount : {}  maxAllowed : {}  suspicious : {}", senderBalance, maxAllowed, amount.compareTo(maxAllowed) > 0);
        
        return amount.compareTo(maxAllowed) > 0;
    }

    private boolean amountCheck(String senderAccountNumber, BigDecimal amount) {
      String avgKey = "fraud:avg_amount" + senderAccountNumber;
      String avgStr = (String) redisTemplate.opsForValue().get(avgKey);

      if(avgStr == null){
        redisTemplate.opsForValue().set(avgKey,amount.toString());
        return false;
      }

      BigDecimal avgAmount = new BigDecimal(avgStr);
      BigDecimal thresholdAmount = avgAmount.multiply(BigDecimal.valueOf(fraudAmountMultiplier));


      //update running avg

      BigDecimal newAvg = avgAmount.add(amount).divide(BigDecimal.valueOf(2));
      redisTemplate.opsForValue().set(avgKey, newAvg.toString());

      return amount.compareTo(thresholdAmount) > 0;

    }

    private boolean isVelocityExceeded(String senderAccountNumber) {
        String key  = "fraud:velocity:" + senderAccountNumber;
        Long count = redisTemplate.opsForValue().increment(key);
        
        if(count != null && count ==1){
            redisTemplate.expire(key,Duration.ofMinutes(1)); // Set expiration for 1 minute
            
        }
        log.info("velocity check amount for account : {} is : {}", senderAccountNumber, count);
        
        return count!=null && count > fraudVelocityCount; // Adjust the threshold as needed
    }


}