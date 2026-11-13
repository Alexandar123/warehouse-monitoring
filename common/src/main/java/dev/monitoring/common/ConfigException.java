package dev.monitoring.common;

import java.util.List;

public class ConfigException extends RuntimeException {

    private final List<String> errors;

    public ConfigException(List<String> errors) {
        super(String.join("; ", errors));
        this.errors = List.copyOf(errors);
    }

    public List<String> errors() {
        return errors;
    }
}
