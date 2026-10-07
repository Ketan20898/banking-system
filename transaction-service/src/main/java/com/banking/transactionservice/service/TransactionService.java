package com.banking.transactionservice.service;

import java.util.List;
import java.util.UUID;

import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.transactionservice.client.AccountServiceClient;
import com.banking.transactionservice.dto.TransactionResponse;
import com.banking.transactionservice.dto.TransferRequest;
import com.banking.transactionservice.entity.Transaction;
import com.banking.transactionservice.entity.TransactionStatus;
import com.banking.transactionservice.entity.TransactionType;
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

   private static final String TRANSACTION_INITIATED_TOPIC = "transaction.intiated";
   private static final String TRANSACTION_COMPLETED_TOPIC = "transaction.completed";
   private static final String TRANSACTION_REFUND_TOPIC = "transaction.refund";



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

    



}
