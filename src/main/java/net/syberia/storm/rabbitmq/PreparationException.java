package net.syberia.storm.rabbitmq;

/**
 * @author Andrey Burov
 */
class PreparationException extends RuntimeException {

    PreparationException(String message, Throwable cause) {
        super(message, cause);
    }
}
