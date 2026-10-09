package com.banking.fraudpaymentservice.controller;

import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.banking.fraudpaymentservice.dto.CreatePaymentRequest;
import com.banking.fraudpaymentservice.dto.PaymentOrderResponse;
import com.banking.fraudpaymentservice.repo.PaymentRepo;
import com.banking.fraudpaymentservice.service.PaymentService;
import com.razorpay.RazorpayException;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

@RestController 
@RequestMapping ("/api/v1/payments")
@RequiredArgsConstructor 
public class PaymentController {
    
    private final PaymentService paymentService;

    @PostMapping ("/createOrder")
    public ResponseEntity<PaymentOrderResponse> createPaymentOrder(
       @Valid  @RequestBody  CreatePaymentRequest paymentRequest
    ) throws RazorpayException{
        return ResponseEntity
        .status(HttpStatus.CREATED)
        .body(paymentService.createPaymentOrder(paymentRequest));
        
    }


    @PostMapping ("/webhook")
    public ResponseEntity<String> handleWebhook(@RequestBody Map<String,Object> payload){
        paymentService.handleWebhook(payload);
        return ResponseEntity.ok("Webhook Processed");
    }

    


}
