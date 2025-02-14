package com.moniepoint.assignment.storage.services;

import com.moniepoint.assignment.storage.entities.Storage;
import com.moniepoint.assignment.storage.exception.KeyNotFoundException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class StorageService implements Storage {

    @Autowired
    private StorageEngine storageEngine;

    @Override
    public String deleteItem(String key) {
        storageEngine.delete(key);
        return "Successfully deleted";
    }

    @Override
    public String addItems(Map<String, String> items) {
        if (items.size() == 1) {
            Map.Entry<String, String> entry = items.entrySet().iterator().next();
            storageEngine.put(entry.getKey(), entry.getValue());
            return "Successfully Created";
        }
        storageEngine.batchPut(items);
        return "Successfully Created";
    }

    @Override
    public String getItem(String key) {
        Object value = storageEngine.read(key);
        if (value == null) throw new KeyNotFoundException();
        return value.toString();
    }

    @Override
    public Map<Object, Object> getRangeItems(String start, String end) {
        return storageEngine.readRange(start, end);
    }
}
