package com.moniepoint.assignment.storage.services;

import com.moniepoint.assignment.storage.entities.Operation;
import com.moniepoint.assignment.storage.entities.OperationType;
import com.moniepoint.assignment.storage.entities.StorageOperation;
import com.moniepoint.assignment.storage.exception.KeyNotFoundException;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
public class StorageEngine<K extends Comparable<? super K>, V> implements StorageOperation<K, V> {
    private final ConcurrentHashMap<K, V> memTable;
    private final WriteAheadLog wal;
    private final LruCache<K, V> cache;
    private final ReadWriteLock lock;
    private final String dataDir;

    public StorageEngine() {
        this.dataDir = new File(getClass().getClassLoader().getResource("data").getFile()).getAbsolutePath();
        this.memTable = new ConcurrentHashMap<>();
        this.wal = new WriteAheadLog(dataDir);
        this.lock = new ReentrantReadWriteLock();
        this.cache = new LruCache<>(1000);
        recoverFromLogs();
    }

    private void recoverFromLogs() {
        try {
            List<Operation> operations = wal.readAllOperations();
            Map<K, V> recoveredState = new HashMap<>();

            for (Operation op : operations) {
                switch (op.type()) {
                    case PUT:
                        recoveredState.put((K) op.key(), (V) op.value());
                        break;
                    case DELETE:
                        recoveredState.remove((K) op.key());
                        break;
                }
            }

            // Update memtable with recovered state
            memTable.putAll(recoveredState);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void put(K key, V value) {
        lock.writeLock().lock();
        try {
            wal.log(new Operation(OperationType.PUT, key, value, null));
            cache.put(key, value);
            memTable.put(key, value);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public V read(K key) {
        lock.readLock().lock();
        try {
            // Check cache first
            V cachedValue = cache.get(key);
            if (cachedValue != null) {
                return cachedValue;
            }

            // Check memory table
            V memValue = memTable.get(key);
            if (memValue != null) {
                cache.put(key, memValue); // Promote to cache
                return memValue;
            }
            throw new KeyNotFoundException();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Map<K, V> readRange(K startKey, K endKey) {
        lock.readLock().lock();
        try {
            Map<K, V> result = new LinkedHashMap<>();

            // Combine results from memory
            Iterator<K> memIter = memTable.keySet().iterator();
            while (memIter.hasNext()) {
                K key = memIter.next();
                if (isInRange(key, startKey, endKey)) {
                    result.put(key, memTable.get(key));
                }
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void batchPut(Map<K, V> entries) {
        lock.writeLock().lock();
        try {
            Operation op = new Operation(OperationType.BATCH_PUT, null, null, entries);
            wal.log(op);
            // Update memory structures
            cache.putAll(entries);
            memTable.putAll(entries);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void delete(K key) {
        lock.writeLock().lock();
        try {
            Operation op = new Operation(OperationType.DELETE, key, null, null);
            if (read(key) == null) throw new KeyNotFoundException();
            wal.log(op);
            cache.remove(key);
            memTable.remove(key);
        } finally {
            lock.writeLock().unlock();
        }
    }

    private boolean isInRange(K key, K startKey, K endKey) {
        return compareKeys(key, startKey) >= 0 && compareKeys(key, endKey) <= 0;
    }

    private int compareKeys(K k1, K k2) {
        if (k1 != null && k2 != null) {
            return k1.compareTo(k2);
        }
        return k1.toString().compareTo(k2.toString());
    }
}
