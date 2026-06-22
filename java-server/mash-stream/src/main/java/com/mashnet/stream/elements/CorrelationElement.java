package com.mashnet.stream.elements;

import com.fasterxml.jackson.core.type.TypeReference;
import com.mashnet.core.utils.JsonUtil;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;

public class CorrelationElement extends MeshElement<String, String> {

    // Частота дискретизації нашого аудіо (8000 Гц)
    private static final int SAMPLE_RATE = 8000;

    @Override
    public void connectInputStreams(Map<String, Flux<String>> inputStreams) {
        Flux<String> streamA = inputStreams.get("input-A");
        Flux<String> streamB = inputStreams.get("input-B");

        if (streamA == null || streamB == null) {
            System.err.println("[CORRELATION] Помилка: потрібно два потоки (input-A та input-B)");
            return;
        }

        Flux<String> processedStream = Flux.zip(streamA, streamB)
                .map(tuple -> {
                    String chunkA = tuple.getT1();
                    String chunkB = tuple.getT2();

                    try {
                        // 1. Десеріалізуємо JSON у списки чисел (Double)
                        List<Double> listA = JsonUtil.MAPPER.readValue(chunkA, new TypeReference<List<Double>>(){});
                        List<Double> listB = JsonUtil.MAPPER.readValue(chunkB, new TypeReference<List<Double>>(){});

                        int n = listA.size();
                        if (n == 0 || n != listB.size()) {
                            return String.format("{\"waveA\": %s, \"waveB\": %s, \"correlation\": [], \"delay\": 0.0}", chunkA, chunkB);
                        }

                        // 2. Реальний алгоритм крос-кореляції
                        // Шукаємо зміщення (lag) в межах +/- 50 семплів
                        int maxLag = 50;
                        double[] correlation = new double[maxLag * 2 + 1];
                        double maxCorr = -Double.MAX_VALUE;
                        int bestLag = 0;

                        for (int lag = -maxLag; lag <= maxLag; lag++) {
                            double sum = 0;
                            for (int i = 0; i < n; i++) {
                                int j = i - lag;
                                // Якщо індекс не виходить за межі масиву, перемножуємо семпли
                                if (j >= 0 && j < n) {
                                    sum += listA.get(i) * listB.get(j);
                                }
                            }

                            correlation[lag + maxLag] = sum;

                            // Запам'ятовуємо зсув, при якому збіг сигналів максимальний
                            if (sum > maxCorr) {
                                maxCorr = sum;
                                bestLag = lag;
                            }
                        }

                        // 3. Нормалізуємо графік кореляції (від 0 до 1) для фронтенду
                        StringBuilder corrBuilder = new StringBuilder("[");
                        for (int i = 0; i < correlation.length; i++) {
                            double normalized = 0.0;
                            if (maxCorr > 0) {
                                // Відсікаємо від'ємні значення, щоб графік мав красивий пік
                                normalized = Math.max(0, correlation[i] / maxCorr);
                            }
                            corrBuilder.append(String.format(java.util.Locale.US, "%.4f", normalized));
                            if (i < correlation.length - 1) corrBuilder.append(",");
                        }
                        corrBuilder.append("]");

                        // 4. Переводимо знайдену затримку (у семплах) в мілісекунди
                        // Формула: (затримка / частоту) * 1000
                        double delayMs = (bestLag / (double) SAMPLE_RATE) * 1000.0;

                        // 5. Віддаємо реальні дані на Дашборд
                        return String.format(java.util.Locale.US,
                                "{\"waveA\": %s, \"waveB\": %s, \"correlation\": %s, \"delay\": %.2f}",
                                chunkA, chunkB, corrBuilder.toString(), delayMs);

                    } catch (Exception e) {
                        System.err.println("[CORRELATION] Помилка обчислень: " + e.getMessage());
                        return String.format("{\"waveA\": %s, \"waveB\": %s, \"correlation\": [], \"delay\": 0.0}", chunkA, chunkB);
                    }
                })
                .share();

        this.outputStreams.put("tdoa-output", processedStream);
    }
}