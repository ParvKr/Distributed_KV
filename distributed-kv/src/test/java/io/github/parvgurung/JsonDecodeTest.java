package io.github.parvgurung;

import io.github.parvgurung.util.Json;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonDecodeTest {
    @Test
    void rejectsTrailingContent() {
        assertThrows(IllegalArgumentException.class, () -> Json.decode("{\"a\":1} garbage"));
    }

    @Test
    void allowsTrailingWhitespace() {
        assertEquals(1, Json.getInt(Json.decode("{\"a\":1}  \n"), "a"));
    }

    @Test
    void rootNullIsRejectedWithIllegalArgument() {
        assertThrows(IllegalArgumentException.class, () -> Json.decode("null"));
    }
}
