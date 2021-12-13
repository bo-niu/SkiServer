package util.cache;

import java.util.HashMap;
import java.util.Map;

public class ServerCache {
    private static ServerCache serverCache = null;
    private Map<String, String> cache = null;

    private ServerCache() {
        cache = new HashMap<>();
    }

    public static ServerCache getInstance() {
        if (serverCache == null) {
            serverCache = new ServerCache();
            return serverCache;
        }

        return serverCache;
    }

    public String request(String key) {
        return cache.getOrDefault(key, null);
    }


    public void put(String key, String value) {
        cache.put(key, value);
    }
}
