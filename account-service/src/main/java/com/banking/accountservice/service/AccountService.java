package com.banking.accountservice.service;

import java.math.BigDecimal;
import java.security.SecureRandom;

import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;

import com.banking.accountservice.dto.AccountResponse;
import com.banking.accountservice.dto.CreateAccountRequest;
import com.banking.accountservice.entity.Account;
import com.banking.accountservice.entity.AccountStatus;
import com.banking.accountservice.entity.AccountType;
import com.banking.accountservice.repo.AccountRepository;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Service
@Validated
@Slf4j
@RequiredArgsConstructor
public class AccountService {

	private final AccountRepository accountRepo;
	private static final SecureRandom secureRandom = new SecureRandom();

	public AccountResponse createAccount(@Valid CreateAccountRequest request) {
		log.info("creating account for :{}", request.getEmail());

		if (accountRepo.existsByEmail(request.getEmail())) {
			throw new RuntimeException("Account already exists for email :" + request.getEmail());
		}
		Account account = new Account();
		account.setAccountHolderName(request.getAccountHolderName());
		account.setEmail(request.getEmail());
		account.setPhone(request.getPhone());
		account.setAccountType(request.getAccountType());
		account.setStatus(request.getAccountStatus());
		account.setBalance(request.getInitialDeposit());
		account.setAccountNumber(generateAccountNumber());
		account.setDailyTransactionLimit(
				request.getAccountType() == AccountType.SAVINGS ? new BigDecimal("100000") : new BigDecimal("500000"));

		Account savedAccount = accountRepo.save(account);
		log.info("Account created : {}", savedAccount.getAccountNumber());
		return mapToResponse(savedAccount);
	}

	private AccountResponse mapToResponse(Account savedAccount) {
		AccountResponse response = new AccountResponse();
		response.setAccountHolderName(savedAccount.getAccountHolderName());
		response.setEmail(savedAccount.getEmail());
		response.setPhone(savedAccount.getPhone());
		response.setAccountType(savedAccount.getAccountType());
		response.setStatus(savedAccount.getStatus());
		response.setBalance(savedAccount.getBalance());
		response.setAccountNumber(savedAccount.getAccountNumber());
		response.setDailyTransactionLimit(savedAccount.getDailyTransactionLimit());

		return response;
	}

	// genetate unique account number 12 digit
	private String generateAccountNumber() {
		String accountNumber;

		do {
			long number = secureRandom.nextLong(100000000000L);
			accountNumber = String.format("%012d", number);
		} while (accountRepo.existsByAccountNumber(accountNumber));

		return accountNumber;
	}

	public AccountResponse getAccount(String accountNumber) {
		log.info("Fetching account for :{}", accountNumber);
		Account account = accountRepo.findByAccountNumber(accountNumber)
				.orElseThrow(() -> new RuntimeException("Account not found for account number :" + accountNumber));
		return mapToResponse(account);
	}

	public BigDecimal getBalance(String accountNumber) {
		log.info("Fetching account balance for :{}", accountNumber);
		Account account = accountRepo.findByAccountNumber(accountNumber)
				.orElseThrow(() -> new RuntimeException("Account not found for account number :" + accountNumber));
		return account.getBalance();
	}

	//Block account - called by fraud detection service by kafka
	public void blockAccount(String accountNumber){
		log.info("Blocking account for :{}", accountNumber);
		Account account = accountRepo.findByAccountNumber(accountNumber)
				.orElseThrow(() -> new RuntimeException("Account not found for account number :" + accountNumber));
		account.setStatus(AccountStatus.BLOCKED);
		accountRepo.save(account);
		log.info("Account blocked : {}", account.getAccountNumber());
	}

	public void deductBalance(String accountNumber, BigDecimal amount) {
		log.info("Deducting balance from account :{}", amount, accountNumber);
		Account account = accountRepo.findByAccountNumber(accountNumber)
				.orElseThrow(() -> new RuntimeException("Account not found for account number :" + accountNumber));

		if(account.getStatus() != AccountStatus.ACTIVE){
			throw new RuntimeException("Account is not active for account number :" + accountNumber);
		}
		
		if (account.getBalance().compareTo(amount) < 0) {
			throw new RuntimeException("Insufficient balance for account number :" + accountNumber);
		}
		account.setBalance(account.getBalance().subtract(amount));
		accountRepo.save(account);
		log.info("Balance deducted : {} for account number : {}", amount, account.getAccountNumber());
		log.info("New balance : {} for account number : {}", account.getBalance(), account.getAccountNumber());
	}


	public void creditBalance(String accountNumber, BigDecimal amount) {
		log.info("Crediting balance to account :{}", amount, accountNumber);
		Account account = accountRepo.findByAccountNumber(accountNumber)
				.orElseThrow(() -> new RuntimeException("Account not found for account number :" + accountNumber));

		if(account.getStatus() != AccountStatus.ACTIVE){
			throw new RuntimeException("Account is not active for account number :" + accountNumber);
		}

		account.setBalance(account.getBalance().add(amount));
		accountRepo.save(account);
		log.info("Balance credited : {} for account number : {}", amount, account.getAccountNumber());
		log.info("New balance : {} for account number : {}", account.getBalance(), account.getAccountNumber());
	}
}
