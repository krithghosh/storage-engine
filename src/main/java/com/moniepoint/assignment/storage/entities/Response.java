package com.moniepoint.assignment.storage.entities;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.io.Serializable;
import java.util.Map;

public record Response(String message,
                       @JsonInclude(JsonInclude.Include.NON_EMPTY) Map<Object, Object> data) implements Serializable {
}