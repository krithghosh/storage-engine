package entities;

import java.io.IOException;
import java.util.Map;

public interface StorageOperation<K, V> {
    void put(K key, V value) throws IOException;

    V read(K key) throws IOException, ClassNotFoundException;

    Map<K, V> readRange(K startKey, K endKey) throws IOException;

    void batchPut(Map<K, V> entries) throws IOException;

    void delete(K key) throws IOException;
}