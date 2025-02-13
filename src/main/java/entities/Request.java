package entities;

import java.io.Serializable;
import java.util.Map;

public record Request(String operation, String key, String value, Map<String, String> entries) implements Serializable {
}
