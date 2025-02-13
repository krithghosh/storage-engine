package entities;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public record Response(String error, String value, List<String> keys,
                       Map<String, String> data) implements Serializable {
}