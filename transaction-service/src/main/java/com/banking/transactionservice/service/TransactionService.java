package com.banking.transactionservice.service;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.jspecify.annotations.Nullable;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
import com.banking.transactionservice.event.TransactionCompletedEvent;
import com.banking.transactionservice.event.TransactionInitiatedEvent;
import com.banking.transactionservice.repo.TransactionRepo;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service 
@Slf4j 
@RequiredArgsConstructor 
public class TransactionService {
    
   private final TransactionRepo transactionRepo;
   private final AccountServiceClient accountServiceClient;
   private final KafkaTemplate<String,Object> kafkaTemplate;
   private final RedisTemplate<String,Object> redisTemplate;

   private static final String TRANSACTION_INITIATED_TOPIC = "transaction.intiated";
   private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
   private static final String TRANSACTION_REFUND_TOPIC = "transaction.refund";
   private static final String FRAUD_DETECTED_TOPIC = "fraud.detected";



//    Saga step- 1 Initiates transaction
//    Deducts from sender via feign
//saves transaction as processing
//public event to kafka for fraud check
//returns 



   public TransactionResponse transfer(TransferRequest transactionRequest) {
       // Implement the transfer logic here
       // Save the transaction to the database
      
       log.info("Saga starts Transaction from :{} -> {} amount : {}", transactionRequest.getSenderAccountNumber(), transactionRequest.getReceiverAccountNumber(), transactionRequest.getAmount());
       
       //SAGA 1 ) feign to deduct from sender
        accountServiceClient.deductBalance(transactionRequest.getSenderAccountNumber(), transactionRequest.getAmount());

        Transaction transaction = new Transaction();

        transaction.setSenderAccountNumber(transactionRequest.getSenderAccountNumber());
        transaction.setReceiverAccountNumber(transactionRequest.getReceiverAccountNumber());
        transaction.setAmount(transactionRequest.getAmount());
        //processing because first we will check fraud service 
        transaction.setStatus(TransactionStatus.PROCESSING);
        transaction.setType(TransactionType.TRANSFER);
        transaction.setDescription(transactionRequest.getDescription());
        transaction.setReferenceNo(UUID.randomUUID().toString());

        Transaction savedTransaction = transactionRepo.save(transaction);

    log.info("Transaction saved as processing : {}",savedTransaction.getId());

    TransactionInitiatedEvent event = new TransactionInitiatedEvent(
        savedTransaction.getId(),
        savedTransaction.getSenderAccountNumber(),
        savedTransaction.getReceiverAccountNumber(),
        savedTransaction.getAmount(),
        savedTransaction.getDescription()
    );

    // Publish the event to Kafka
    kafkaTemplate.send(TRANSACTION_INITIATED_TOPIC, event);
    log.info("Saga step 2: TransactionInitiatedEvent published to Kafka for transactionId: {}", savedTransaction.getId());


    return mapToTransactionResponse(savedTransaction);


   }

   public TransactionResponse getTransaction(String transactionId) {
        Transaction transaction = transactionRepo.findById(transactionId)
                .orElseThrow(() -> new RuntimeException("Transaction not found with id: " + transactionId));
        return mapToTransactionResponse(transaction);
    }


    public List<TransactionResponse> getTransactionHistory(String accountNumber) {
     return  transactionRepo.findBySenderAccountNumberOrderByCreatedAtDesc(accountNumber)
                .stream()
                .map(this::mapToTransactionResponse)
                .toList();
                
    }



   private TransactionResponse mapToTransactionResponse(Transaction savedTransaction) {
            TransactionResponse transactionResponse = new TransactionResponse();
            transactionResponse.setId(savedTransaction.getId());    
            transactionResponse.setSenderAccountNumber(savedTransaction.getSenderAccountNumber());
            transactionResponse.setReceiverAccountNumber(savedTransaction.getReceiverAccountNumber());  
            transactionResponse.setAmount(savedTransaction.getAmount());
            transactionResponse.setStatus(savedTransaction.getStatus());
            transactionResponse.setType(savedTransaction.getType());
            transactionResponse.setDescription(savedTransaction.getDescription());
            transactionResponse.setReferenceNo(savedTransaction.getReferenceNo());
            return transactionResponse;
    }

   public Object verifyOtp(String transactionId, String otp) {
        log.info("Otp for transaction verification : {}", otp);

        Transaction transaction = transactionRepo.findById(transactionId)
                        .orElseThrow(() -> new RuntimeException("Transaction not found :"+transactionId));

        String otpKey = "verification:otp"+transactionId;
        String storedOtp =(String) redisTemplate.opsForValue().get(otpKey);                    

        if(storedOtp ==null ){
            log.warn("Otp expired for transaction id: "+transactionId);
            compensateTransaction(transaction, "OTP expired transaction cancelled and amount refunded");
            return mapToTransactionResponse(transaction);

        }

        if(!storedOtp.equals(otp)){
            log.warn("Wrong otp and refunding: {}", transactionId);
            redisTemplate.delete(otpKey);
            blockAccountAndCompensate(transaction, 
                "Wrong otp - transaction cancelled",
                "Account blocked for security reasons"
            );
        }

            log.info("Otp verified transaction processed");
            redisTemplate.delete(otpKey);
            completeTransaction(transaction);
            return mapToTransactionResponse(transaction);

   }

   private void completeTransaction(Transaction transaction) {
            //complete the transaction 
            transaction.setStatus(TransactionStatus.COMPLETED);
            transaction.setCompletedAt(LocalDateTime.now());

            transactionRepo.save(transaction);

            TransactionCompletedEvent completedEvent = new
             TransactionCompletedEvent(
                transaction.getId(),
                transaction.getSenderAccountNumber(),
                transaction.getReceiverAccountNumber(),
                transaction.getAmount(),
                transaction.getDescription()
             );

             kafkaTemplate.send(TRANSACTION_COMPLETED_TOPIC, transaction.getId(), completedEvent);

             log.info("Saga transaction complete - {}",transaction.getId());

}

   private void blockAccountAndCompensate(Transaction transaction, String string, String reason) {
    //publish fraud.detedcted event and account service will block account

        Map<String,Object> fraudEvent  = new HashMap<>();
            fraudEvent.put("TransactionId",transaction.getId());
            fraudEvent.put("SenderAccountNumber",transaction.getSenderAccountNumber());
            fraudEvent.put("Reason", transaction.getFailureReason());

            kafkaTemplate.send(FRAUD_DETECTED_TOPIC,transaction.getSenderAccountNumber(),fraudEvent);

            log.warn("fraud detected published for account : {} reason : {} , Kindly connect with bank", transaction.getSenderAccountNumber(),reason);

            //saga compensate refund sender

            compensateTransaction(transaction, reason);
}

   private void compensateTransaction(Transaction transaction, String reason) {

    log.warn("saga compensate refunding to :{} amount : {}", transaction.getSenderAccountNumber(), transaction.getAmount());

    //credit money back to our sender

    accountServiceClient.creditBalance(transaction.getSenderAccountNumber(), transaction.getAmount());

    transaction.setFailureReason(reason+ " Saga Compenstation executed at "+LocalDateTime.now());
    transaction.setStatus(TransactionStatus.FLAGGED);

    transactionRepo.save(transaction);

    //public refund event and nofify sender


    Map<String,Object> refundEvent = new HashMap<>();

    refundEvent.put("transactionId",transaction.getId());
    refundEvent.put("Sender Account number", transaction.getSenderAccountNumber());
    refundEvent.put("Amount",transaction.getAmount());
    refundEvent.put("Reason",reason);

    kafkaTemplate.send(TRANSACTION_REFUND_TOPIC,transaction.getId(),refundEvent);

    log.info("Saga compensation completed {} refunded to :{}",transaction.getAmount(), transaction.getSenderAccountNumber());


    


   }

   public void processCleanResult(String transactionId) {

    Transaction transaction = transactionRepo.findById(transactionId)
                            .orElseThrow(() -> new RuntimeException("Error in transaction"));
                if(transaction.getStatus() != TransactionStatus.PROCESSING){
                    log.warn("Transaction : {} is not processing skipping ",transactionId);
                    return;
                }
   
                            completeTransaction(transaction);
   }

    



}
