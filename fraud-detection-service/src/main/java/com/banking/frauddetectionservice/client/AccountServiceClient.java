package com.banking.frauddetectionservice.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;

@FeignClient(name = "account-service", url = "${account.service.url}")
public interface AccountServiceClient {
    
    @GetMapping ("/api/v1/accounts/{accountNumber}/balance")
    String getAccountBalance(String accountNumber);
}
