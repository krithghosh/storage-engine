package entities;

import java.util.Map;

public record Operation<K, V>(OperationType type, K key, V value, Map<K, V> batchData) {
}
