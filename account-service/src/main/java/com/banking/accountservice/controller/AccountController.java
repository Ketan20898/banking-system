package com.banking.accountservice.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.dto.CreateAccountResponse;
import com.banking.accountservice.service.AccountService;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/v1/accounts")
@Slf4j
@RequiredArgsConstructor
public class AccountController {
	
	private final AccountService accountService;
	
	@PostMapping()
	public ResponseEntity<CreateAccountResponse> createAccountReq(@Valid @RequestBody CreateAccountRequest request){
		return ResponseEntity.status(HttpStatus.CREATED).body(accountService.createAccount(request));
	}
	
	public ResponseEntity<CreateAccountResponse> getAccount(@PathVariable String accountNumber){
		return accountService.get
	}

}
