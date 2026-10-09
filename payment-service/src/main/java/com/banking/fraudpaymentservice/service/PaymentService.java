package com.banking.fraudpaymentservice.service;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.json.JSONObject;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.banking.fraudpaymentservice.dto.CreatePaymentRequest;
import com.banking.fraudpaymentservice.dto.PaymentOrderResponse;
import com.banking.fraudpaymentservice.entity.Payment;
import com.banking.fraudpaymentservice.entity.PaymentStatus;
import com.banking.fraudpaymentservice.repo.PaymentRepo;
import com.razorpay.Order;
import com.razorpay.RazorpayClient;
import com.razorpay.RazorpayException;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * PaymentService
 */
@Service 
@Slf4j 
@RequiredArgsConstructor 
public class PaymentService {

    private final PaymentRepo paymentRepo;
    private final KafkaTemplate<String,Object> kafkaTemplate;
    
    private final RazorpayClient razorpayClient;
    
    private static final String PAYMENT_COMPLETED_EVENT = "payment.completed";
    private static final String PAYMENT_FAILED_EVENT = "payment.failed";

    @Value ("${razorpay.key-id}")
    private String keyId;
    @Value ("${razorpay.key-secret}")
    private String keySecret;


    //create order in razor pay
    //save payment record in db
    //return order details to fe
    //payment shows on razorpay checkout page
    //razorpay call webhook
    public PaymentOrderResponse createPaymentOrder(CreatePaymentRequest paymentRequest) throws RazorpayException {
        
        //creating payment for account and 
        log.info("creating payment for account : {} amount : {}", paymentRequest.getAccountNumber(), paymentRequest.getAmount());

        RazorpayClient razorpayClient = new RazorpayClient(keyId,keySecret);

        //convert in smaller currenct like 1 rs =100paise
        int convertedAmount = paymentRequest.getAmount()
        .multiply(BigDecimal.valueOf(100)).intValue();

        JSONObject orderReq = new JSONObject();

        orderReq.put("amount",convertedAmount);
        orderReq.put("currency","USD/INR");
        orderReq.put("receipt", "rcpt_"+System.currentTimeMillis()+UUID.randomUUID().toString()
                .replace("-", "").substring(0,10));
        
        Order razorpayOrder = razorpayClient.orders.create(orderReq);

        log.info("razorpay order created", razorpayOrder.get("id").toString());

        //save payment record in DB

        Payment payment = new Payment();

        payment.setRazorpayOrderId(razorpayOrder.get("id").toString());
        payment.setAccountNumber(paymentRequest.getAccountNumber());
        payment.setAmount(paymentRequest.getAmount());
        payment.setCurrency("USD/INR");
        payment.setStatus(PaymentStatus.CREATED);
        payment.setDescription(paymentRequest.getDescription());

        Payment savedPayment = paymentRepo.save(payment);

        return new PaymentOrderResponse(savedPayment.getId(), savedPayment.getRazorpayOrderId()
        , savedPayment.getAmount(), savedPayment.getCurrency(), "CREATED", keyId);


      
    }

    public void handleWebhook(Map<String,Object> payload) {
        log.info("received webhook event : {}",payload.get("event"));
        String event = (String) payload.get("event");
        if("payment.captured".equals(event))
        {
            handlePaymentSuccess(payload);
        }else if("payment.failed".equals(event)){
            handlePaymentFailed(payload);
        }
    }

    private void handlePaymentFailed(Map<String,Object> payload) {
         Map<String,Object> paymentData = extractPaymentData(payload);

        String orderId = (String) paymentData.get("order-id");
        

         Payment payment = paymentRepo.findByRazorpayOrderId(orderId)
            .orElseThrow(() -> new RuntimeException("Payment not found for order :"+orderId));

             payment.setStatus(PaymentStatus.COMPLETED);
            payment.setPaymentFailureReason("payment failed via razorpay");
            paymentRepo.save(payment);

             Map<String,Object> event = new HashMap();
            
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("failure-reason","Payment failed via razorpay");

            kafkaTemplate.send(PAYMENT_FAILED_EVENT,payment.getId(),event);

    }

    private void handlePaymentSuccess(Map<String,Object> payload) {
       try {
        Map<String,Object> paymentData = extractPaymentData(payload);

        String orderId = (String) paymentData.get("order-id");
        String paymentId = (String) paymentData.get("id");

        Payment payment = paymentRepo.findByRazorpayOrderId(orderId)
            .orElseThrow(() -> new RuntimeException("Payment not found for order :"+orderId));


            payment.setRazorpayPaymentId(paymentId);
            payment.setStatus(PaymentStatus.COMPLETED);

            paymentRepo.save(payment);

            //public payment success event to kafka

            Map<String,Object> event = new HashMap();
            
            event.put("paymentId", payment.getId());
            event.put("accountNumber", payment.getAccountNumber());
            event.put("amount", payment.getAmount());
            event.put("razorpayPaymentId",payment.getRazorpayPaymentId());

            kafkaTemplate.send(PAYMENT_COMPLETED_EVENT,payment.getId(),event);

            log.info("Payment completed : ",payment.getId());
       } catch (Exception e) {
        log.info("Error in payment : {}",e.getMessage());
       }
    }

    private Map<String, Object> extractPaymentData(Map<String,Object> payload) {
        Map<String,Object> entity = (Map<String,Object>) payload.get(payload);

        Map<String,Object> paymentWrapper =(Map<String,Object>) entity.get("payment");

        return (Map<String,Object>) paymentWrapper.get("entity");
    }

}
