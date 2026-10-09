package com.banking.fraudpaymentservice.repo;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.banking.fraudpaymentservice.entity.Payment;

public interface PaymentRepo extends JpaRepository<Payment,String> {

    Optional<Payment> findByRazorpayOrderId(String orderId);
    

}
