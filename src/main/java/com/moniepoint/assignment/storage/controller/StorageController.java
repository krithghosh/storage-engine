package com.moniepoint.assignment.storage.controller;

import com.moniepoint.assignment.storage.entities.Response;
import com.moniepoint.assignment.storage.exception.KeyAlreadyPresentException;
import com.moniepoint.assignment.storage.exception.KeyNotFoundException;
import com.moniepoint.assignment.storage.services.StorageService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Collections;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class StorageController {

    @Autowired
    private StorageService storageService;

    @RequestMapping(method = RequestMethod.POST, value = "/v1/items", produces = "application/json")
    @Operation(summary = "Create batch items", description = "This method creates batched items", responses = {@ApiResponse(responseCode = "200", description = "Created")})
    public ResponseEntity<Response> addItems(@RequestBody Map<String, String> entries) {
        try {
            String response = storageService.addItems(entries);
            return new ResponseEntity<>(new Response(response, Collections.emptyMap()), HttpStatus.CREATED);
        } catch (KeyAlreadyPresentException e) {
            return new ResponseEntity<>(new Response("Key already present", Collections.emptyMap()), HttpStatus.CREATED);
        }
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/v1/item", produces = "application/json")
    @Operation(summary = "Delete item", description = "This method deletes items", responses = {@ApiResponse(responseCode = "200", description = "Deleted")})
    public ResponseEntity<Response> deleteItem(@RequestParam(defaultValue = "city") String key) {
        try {
            String response = storageService.deleteItem(key);
            return new ResponseEntity<>(new Response("Success", Collections.emptyMap()), HttpStatus.OK);
        } catch (KeyNotFoundException e) {
            return new ResponseEntity<>(new Response("Key not found", Collections.emptyMap()), HttpStatus.CREATED);
        }
    }

    @RequestMapping(method = RequestMethod.GET, value = "/v1/item", produces = "application/json")
    @Operation(summary = "Fetch item", description = "This method fetches value for given key", responses = {@ApiResponse(responseCode = "200", description = "Fetches value for key")})
    public ResponseEntity<Response> getItem(@RequestParam(defaultValue = "city") String key) {
        try {
            String response = storageService.getItem(key);
            return new ResponseEntity<>(new Response("Success", Map.of(key, response)), HttpStatus.OK);
        } catch (KeyNotFoundException e) {
            return new ResponseEntity<>(new Response("Key not found", Collections.emptyMap()), HttpStatus.CREATED);
        }
    }

    @RequestMapping(method = RequestMethod.GET, value = "/v1/items", produces = "application/json")
    @Operation(summary = "Fetches item in range", description = "This method fetches value for given keys in ranges", responses = {@ApiResponse(responseCode = "200", description = "Fetches values for keys provided")})
    public ResponseEntity<Response> getItems(@RequestParam(defaultValue = "city") String start, @RequestParam(defaultValue = "state") String end) {
        Map<Object, Object> response = storageService.getRangeItems(start, end);
        return new ResponseEntity<>(new Response(response.isEmpty() ? "No data found for given range" : "Success", response), HttpStatus.OK);
    }
}
