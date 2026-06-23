package com.emailnotificationms.ws.emailnotification.error;

public class RetryableException  extends RuntimeException{
    public RetryableException(String message) {
        super(message); // to accept the custom error message
    }

    public RetryableException(Throwable cause) {
        super(cause); // accept of the original exception that caused an error
    }
}
