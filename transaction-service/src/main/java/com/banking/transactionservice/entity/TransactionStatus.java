package com.banking.transactionservice.entity;

/**
 * TransactionStatus
 */
public enum TransactionStatus {

    PENDING,
    PROCESSING,
    PENDING_VERIFICATION,
    COMPLETED,
    FAILED,
    FLAGGED

}
