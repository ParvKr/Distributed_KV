package io.github.parvgurung.rpc;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Optional;
import java.util.function.Function;

public class RpcClient {
    private static final Duration REQUEST_TIMEOUT = Duration.ofMillis(300);
    // A snapshot carries the whole state machine, so it gets far longer than a heartbeat does.
    private static final Duration SNAPSHOT_TIMEOUT = Duration.ofSeconds(10);
    private final HttpClient httpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_1_1)
        .connectTimeout(Duration.ofMillis(200))
        .build();

    public Optional<RequestVoteResponse> sendRequestVote(String baseUrl, RequestVoteRequest request) {
        return post(baseUrl + "/raft/requestVote", request.toJson())
            .flatMap(body -> parse(body, RequestVoteResponse::fromJson));
    }

    public Optional<AppendEntriesResponse> sendAppendEntries(String baseUrl, AppendEntriesRequest request) {
        return post(baseUrl + "/raft/appendEntries", request.toJson())
            .flatMap(body -> parse(body, AppendEntriesResponse::fromJson));
    }

    public Optional<InstallSnapshotResponse> sendInstallSnapshot(String baseUrl, InstallSnapshotRequest request) {
        return post(baseUrl + "/raft/installSnapshot", request.toJson(), SNAPSHOT_TIMEOUT)
            .flatMap(body -> parse(body, InstallSnapshotResponse::fromJson));
    }

    private static <T> Optional<T> parse(String body, Function<String, T> parser) {
        try {
            return Optional.of(parser.apply(body));
        } catch (RuntimeException e) {
            return Optional.empty();
        }
    }

    private Optional<String> post(String url, String jsonBody) {
        return post(url, jsonBody, REQUEST_TIMEOUT);
    }

    private Optional<String> post(String url, String jsonBody, Duration timeout) {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(timeout)
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