package com.deutscheboerse.risk.dave.utils;

import com.deutscheboerse.risk.dave.MainVerticleIT;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DataAnonymizer {

    private Random random = new Random();
    private ConcurrentHashMap<String, String> anonymizedClearers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> anonymizedMembers = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, String> anonymizedPools = new ConcurrentHashMap<>();

    private void anonymize() {
        IntStream.rangeClosed(1, 2).forEach(i -> this.anonymizeFile("accountMargin", i));
        IntStream.rangeClosed(1, 2).forEach(i -> this.anonymizeFile("liquiGroupMargin", i));
        IntStream.rangeClosed(1, 2).forEach(i -> this.anonymizeFile("liquiGroupSplitMargin", i));
        IntStream.rangeClosed(1, 2).forEach(i -> this.anonymizeFile("poolMargin", i));
        IntStream.rangeClosed(1, 2).forEach(i -> this.anonymizeFile("positionReport", i));
        IntStream.rangeClosed(1, 2).forEach(i -> this.anonymizeFile("riskLimitUtilization", i));
    }

    private void anonymizeFile(String folderName, int ttsaveNo) {
        JsonArray anonymizedArray = new JsonArray();
        DataHelper.readTTSaveFile(folderName, ttsaveNo)
                .forEach(json -> {
                    if (json.containsKey("clearer")) {
                        String clearer = json.getString("clearer");
                        String anonymizedClearer = anonymizedClearers.computeIfAbsent(clearer, (c) -> this.generateRandomString(c.length()));
                        json.put("clearer", anonymizedClearer);
                    }
                    if (json.containsKey("member")) {
                        String member = json.getString("member");
                        String anonymizedMember = anonymizedMembers.computeIfAbsent(member, (m) -> this.generateRandomString(m.length()));
                        json.put("member", anonymizedMember);
                    }
                    if (json.containsKey("pool")) {
                        String pool = json.getString("pool");
                        String anonymizedPool = anonymizedPools.computeIfAbsent(pool, (p) -> this.generateRandomString(p.length()));
                        json.put("pool", anonymizedPool);
                    }
                    anonymizedArray.add(json);
                });
        String jsonPath = String.format("%s/snapshot-anonymized-%03d.json", MainVerticleIT.class.getResource(folderName).getPath(), ttsaveNo);
        this.write(jsonPath, anonymizedArray);
    }

    private String generateRandomString(int length) {
        return this.random.ints(65, 91)
                .limit(length)
                .mapToObj(i -> (char) i)
                .collect(StringBuilder::new, StringBuilder::append, StringBuilder::append)
                .toString();
    }

    private void write(String path, JsonArray jsonArray) {
        String output = jsonArray.stream()
                .map(o -> (JsonObject) o)
                .map(json -> json.encode())
                .collect(Collectors.joining(",\n", "[\n", "\n]"));
        try (PrintWriter writer = new PrintWriter(new FileWriter(path))) {
            writer.println(output);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        new DataAnonymizer().anonymize();
    }
}
