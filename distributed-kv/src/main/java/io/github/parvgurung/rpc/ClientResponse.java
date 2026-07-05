package io.github.parvgurung.rpc;

import io.github.parvgurung.util.Json;

import java.util.LinkedHashMap;
import java.util.Map;

public record ClientResponse(
    boolean success,
    String value,
    String error,
    String leaderHint
) {
    public static ClientResponse OK(String value) {
        return new ClientResponse(true, value, null, null);
    }

    public static ClientResponse notLeader(String leaderHint) {
        return new ClientResponse(false, null, "not leader", leaderHint);
    }

    public static ClientResponse error(String message) {
        return new ClientResponse(false, null, message, null);
    }
    
    public String toJson() {
        Map<String, Object> obj = new LinkedHashMap<>();
        obj.put("success", success);
        obj.put("value", value);
        obj.put("error", error);
        obj.put("leaderHint", leaderHint);
        return Json.encode(obj);
    }

    public static ClientResponse fromJson(String json) {
        Map<String, Object> obj = Json.decode(json);
        return new ClientResponse(
            Json.getBoolean(obj, "success"),
            Json.getString(obj, "value"),
            Json.getString(obj, "error"),
            Json.getString(obj, "leaderHint")
        );
    }
}
