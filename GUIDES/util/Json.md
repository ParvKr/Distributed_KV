Page - 002

A Zero-Dependency, Custom JSON Serializer and Recursive Descent Parser

[CODE LINK - util/Json.java](https://github.com/ParvKr/Distributed_KV/blob/main/distributed-kv/src/main/java/io/github/parvgurung/util/Json.java)

#### RESPONSIBILITY

- Encoding - Handles the translation of in-memory data structures (`Map`, `List`, primitives) to raw JSON text strings.
- Decoding - Transforms incoming raw wire string data back into Java objects.

#### STRUCTURAL ARCHITECTURE

1. Private Constructor
    - `private Json() {}`
    - **Purpose:** Prevents instantiation. Enforces that this utility class contains only pure, stateless static functions.
2. The Inner `Parser` Class
    - Encapsulates state (`String s`, `int pos`) to track index boundaries sequentially across the payload.
    - Implements a **Recursive Descent Parser**, dynamically matching token characters (`{`, `[`, `"`, numbers) to construct equivalent objects.
    - Uses a `LinkedHashMap` inside `parseObject()` to preserve the exact insertion order of elements parsed from the JSON string.

**CORE METHODS**

1. Encoding Operations (`encode()`)
    - **`encodeValue(Object value, StringBuilder sb)`**: Acts as a type switch. Evaluates variable wrappers using pattern matching (`instanceof String s`) and routes execution to specific helper functions.
    - **`encodeString(String s, StringBuilder sb)`**: Protects against payload malformation. Safely escapes control characters (`\n`, `\t`, `\"`, `\\`) and uses unicode escaping (`\\u%04x`) for unprintable control tokens lower than `0x20` or higher than `0x7E`.
2. Decoding Operations (`decode()`)
    - **`parseValue()`**: The foundational switch dispatcher. Reads the character under the pointer using `peek()` to jump execution into specific type token trees.
    - **`parseString()`**: Scans text until finding the unescaped closing quote mark (`"`). Decodes backslash escape codes (like converting `\n` characters back into literal newlines).
    - **`parseNumber()`**: Iterates through digits, negative sign symbols, decimals, and scientific exponents (`e`/`E`). Smart-casts outcomes into `Integer`, `Long`, or `Double` primitives based on scale to preserve memory overhead.
3. Strongly Typed Getters (`getInt()`, `getString()`, etc.)
    - Combats type erasure caused by standard `Map<String, Object>` structures. Defensively extracts elements and handles safety checks (like downcasting a `Long` to an `int` if it fits).

#### FAQs

1. Why did we write our own JSON parser instead of pulling in a standard dependency like Jackson or Gson?
    - To guarantee a system with absolutely zero external compile-time dependencies, minimizing footprint overhead.
    - More importantly, it demonstrates a ground-up understanding of lexical tokens and deterministic parsing—essential skills when managing raw TCP/gRPC frame structures in a Raft consensus cluster.
2. What parsing algorithm strategy does our `Parser` implementation follow?
    - It is a **Recursive Descent Parser** (an LL(1) style parser).
    - It uses a single cursor token lookahead (`peek()`) to deterministically decide which method branch (`parseObject`, `parseArray`, `parseNumber`) to call next without needing back-tracking algorithms.
3. Why does `parseNumber()` return different object types (`Integer`, `Long`, `Double`) instead of just wrapping everything in a `Double`?
    - Memory optimization and structural correctness. In a Raft system, term counts and index markers are absolute whole integers.
    - Forcing them into floating-point `Double` representations introduces rounding precision hazards.
    - Choosing small types first preserves heap efficiency.
4. Is our `Json` class thread-safe?
    - Yes, because the outer class is entirely stateless.
    - The `Parser` class maintains internal mutable pointers (`pos`), but it is instantiated freshly *inside* the execution frame of the `decode()` method stack.
    - Because a `Parser` instance is confined to a single thread execution stack and never shared, the implementation is completely thread-safe.
5. How does `encodeString()` prevent data corruption or breaking the JSON payload layout?
    - It performs strict character escaping.
    - If a string field inside the Key-Value store contains a literal double quote (`"`) or newline (`\n`), writing it unescaped would break the structure of the JSON text wire payload.
    - The method intercepts these and inserts backslashes (`\"`, `\n`) defensively.
  
[NEXT - \[003\]statemachine/commandTypes.md](https://github.com/ParvKr/Distributed_KV/blob/main/GUIDES/statemachine/commandType.md)
