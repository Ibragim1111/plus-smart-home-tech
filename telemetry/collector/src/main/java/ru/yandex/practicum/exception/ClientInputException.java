package ru.yandex.practicum.exception;

public class ClientInputException extends RuntimeException {

    public ClientInputException(String message) {
        super(message);
    }
}