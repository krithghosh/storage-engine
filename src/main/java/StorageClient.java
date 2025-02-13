import services.StorageEngine;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StorageClient {

    private final static String INPUT_KEY = "Key: ";
    private final static String INPUT_VALUE = "Value: ";
    private final static String INPUT_START = "Start: ";
    private final static String INPUT_END = "End: ";
    private final static String INPUT_OPERATION = "Enter operation (or 'exit' to quit): ";
    private final static Set<String> operations = Set.of("put", "get", "batch", "delete", "range", "exit");

    public static void main(String[] args) {
        StorageEngine storageEngine = new StorageEngine();
        startInputProcess(storageEngine);
    }

    public static void startInputProcess(StorageEngine storageEngine) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            while (true) {
                String operation = input(reader, INPUT_OPERATION);
                if (!operations.contains(operation)) {
                    System.out.println("Not a valid operation");
                    continue;
                }
                if (operation == null || operation.equalsIgnoreCase("exit")) {
                    break;
                }
                processRequest(reader, operation, storageEngine);

            }
        } catch (IOException e) {
            System.err.println("Error reading input: " + e.getMessage());
        }
    }

    public static String input(BufferedReader reader, String message) throws IOException {
        System.out.println(message);
        return reader.readLine();
    }

    private static void processRequest(BufferedReader reader, String operation, StorageEngine storageEngine) throws IOException {
        switch (operation) {
            case "put":
                storageEngine.put(input(reader, INPUT_KEY), input(reader, INPUT_VALUE));
                break;
            case "get":
                Object value = storageEngine.read(input(reader, INPUT_KEY));
                if (value == null) {
                    System.out.println("Key not found");
                    return;
                }
                System.out.println("Result: " + value);
                break;
            case "range":
                Map<Object, Object> values = storageEngine.readRange(input(reader, INPUT_START), input(reader, INPUT_END));
                System.out.println("Result: " + values);
                break;
            case "batch":
                int size = Integer.parseInt(input(reader, "Enter batch size: "));
                Map<String, String> entities = new HashMap<>();
                for (int i = 0; i < size; i++) {
                    entities.put(input(reader, INPUT_KEY), input(reader, INPUT_VALUE));
                }
                storageEngine.batchPut(entities);
                break;
            case "delete":
                storageEngine.delete(input(reader, INPUT_KEY));
                break;
        }
    }
}
