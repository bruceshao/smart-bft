package com.x.farmer.bft.exception;

public class BftServiceException extends RuntimeException {

    public BftServiceException() {
        super();
    }

    public BftServiceException(String s) {
        super(s);
    }

    public BftServiceException(String message, Throwable cause) {
        super(message, cause);
    }


    public BftServiceException(Throwable cause) {
        super(cause);
    }
}
