package com.moniepoint.assignment.storage.services;

import com.moniepoint.assignment.storage.entities.Operation;
import com.moniepoint.assignment.storage.entities.OperationType;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.nio.channels.FileChannel;

class WriteAheadLog<K, V> implements AutoCloseable {
    private final ByteBuffer buffer;
    private final FileChannel fileChannel;
    private final File logFile;

    public WriteAheadLog() {
        try {
            logFile = new File(System.getProperty("user.dir") + "/wal.log");
            fileChannel = FileChannel.open(logFile.toPath(), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            buffer = ByteBuffer.allocateDirect(8192); // 8KB buffer
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized void log(Operation operation) {
        try {
            switch (operation.type()) {
                case PUT:
                    serializePair(OperationType.PUT, (K) operation.key(), (V) operation.value());
                    break;
                case BATCH_PUT:
                    Map<Object, Object> map = operation.batchData();
                    for (Map.Entry<Object, Object> entry : map.entrySet()) {
                        serializePair(OperationType.PUT, (K) entry.getKey(), (V) entry.getValue());
                    }
                    break;
                case DELETE:
                    serializePair(OperationType.DELETE, (K) operation.key(), null);
                    break;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void serializePair(OperationType type, K key, V value) throws IOException {
        buffer.clear();

        buffer.putInt(type.ordinal());
        buffer.putInt(key.toString().length());
        buffer.put(key.toString().getBytes());

        if (value == null) {
            buffer.putInt(-1);
        } else {
            buffer.putInt(value.toString().length());
            buffer.put(value.toString().getBytes());
        }

        buffer.flip();
        fileChannel.write(buffer);
        fileChannel.force(true);
    }

    public List<Operation> readAllOperations() throws IOException {
        List<Operation> operations = new ArrayList<>();
        buffer.clear();
        FileChannel fileChannel = FileChannel.open(logFile.toPath(), StandardOpenOption.READ);

        while (fileChannel.read(buffer) != -1 || buffer.hasRemaining()) {
            buffer.flip();

            if (buffer.remaining() < 4) break;
            OperationType type;
            try {
                type = OperationType.values()[buffer.getInt()];
            } catch (ArrayIndexOutOfBoundsException e) {
                break;
            }

            int length = buffer.getInt();

            if (buffer.remaining() < length) break;
            byte[] wordBytes = new byte[length];
            buffer.get(wordBytes);
            String key = new String(wordBytes);

            length = buffer.getInt();

            String value = null;
            if (length != -1) {
                if (buffer.remaining() < length) break;
                wordBytes = new byte[length];
                buffer.get(wordBytes);
                value = new String(wordBytes);
            }
            buffer.compact();

            operations.add(new Operation(type, key, value, Collections.emptyMap()));
        }
        return operations;
    }

    @Override
    public void close() throws Exception {
        fileChannel.close();
    }
}