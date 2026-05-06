package com.mashnet.core.utils;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonUtil {

    // Єдиний екземпляр ObjectMapper для всього проєкту (Singleton патерн)
    public static final ObjectMapper MAPPER = new ObjectMapper();

    // Приватний конструктор, щоб ніхто не міг створити об'єкт через new JsonUtil()
    private JsonUtil() {
    }
}