package dev.monitoring.common;

import java.util.List;

/** Thrown by the config factories of both services; carries every problem found, not just the first. */
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
