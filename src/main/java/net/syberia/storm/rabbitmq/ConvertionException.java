package net.syberia.storm.rabbitmq;

/**
 * @author Andrey Burov
 */
@SuppressWarnings("unused")
public class ConvertionException extends Exception {

    public ConvertionException() {
        // no operation
    }

    public ConvertionException(String message) {
        super(message);
    }

    public ConvertionException(String message, Throwable cause) {
        super(message, cause);
    }

    public ConvertionException(Throwable cause) {
        super(cause);
    }

    public ConvertionException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
