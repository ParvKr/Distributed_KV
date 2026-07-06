        package io.github.parvgurung.rpc;

        import java.io.IOException;
        import java.net.URI;
        import java.net.http.HttpClient;
        import java.net.http.HttpRequest;
        import java.net.http.HttpResponse;
        import java.time.Duration;
        import java.util.Optional;

        public final class RpcClient {
            private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(300);
            private final HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(200))
                .build();

            public Optional<RequestVoteResponse> sendRequestVote(String baseUrl, RequestVoteRequest request) {
                return post(baseUrl + "/raft/requestVote", request.toJson())
                    .map(RequestVoteResponse::fromJson);
            }

            public Optional<AppendEntriesResponse> sendAppendEntries(String baseUrl, AppendEntriesRequest request) {
                return post(baseUrl + "/raft/appendEntries", request.toJson())
                    .map(AppendEntriesResponse::fromJson);
            }

            private Optional<String> post(String url, String jsonBody) {
                try {
                    HttpRequest request = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(REQUEST_TIMEOUT)
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                        .build();
                    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                    if (response.statusCode() != 200)
                        return Optional.empty();
                    return Optional.of(response.body());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return Optional.empty();
                } catch (IOException e) {
                    return Optional.empty();
                }
            }
        }