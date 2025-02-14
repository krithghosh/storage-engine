package com.moniepoint.assignment.storage.entities;

import java.io.Serializable;

public record Request(String key, String value) implements Serializable {
}
