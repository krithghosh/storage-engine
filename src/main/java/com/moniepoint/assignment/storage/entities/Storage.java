package com.moniepoint.assignment.storage.entities;

import java.util.Map;

public interface Storage {

    String deleteItem(String key);

    String addItems(Map<String, String> items);

    String getItem(String key);

    Map<Object, Object> getRangeItems(String start, String end);
}
