package org.neo4j.dbcopy.bolt;

import org.neo4j.dbcopy.DataReader;
import org.neo4j.driver.Driver;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.reactivestreams.ReactiveResult;
import org.neo4j.driver.reactivestreams.ReactiveSession;
import org.neo4j.driver.types.Node;
import org.neo4j.driver.types.Relationship;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.function.Supplier;

public class BoltReader implements DataReader {

    protected static final Logger LOG = LoggerFactory.getLogger(BoltReader.class);

    private final Driver driver;
    private final String databaseName;

    public BoltReader(Driver driver, String databaseName) {
        this.driver = driver;
        this.databaseName = databaseName;
    }

    @Override
    public Flux<Node> readNodes() {
        return Flux.usingWhen(Mono.fromSupplier(getRxSession()),
                session -> session.executeRead(tx -> Flux.from(tx.run("MATCH (n) RETURN n")).flatMap(ReactiveResult::records)
                        .map(record -> record.get(0).asNode())
                        .doOnSubscribe(it -> LOG.info("Start reading nodes")))
                , ReactiveSession::close);
    }

    @Override
    public Flux<Relationship> readRelationships() {
        return Flux.usingWhen(Mono.fromSupplier(getRxSession()),
                session -> session.executeRead(tx -> Mono.from(tx.run("MATCH ()-[rel]->() RETURN rel")).flatMapMany(ReactiveResult::records)
                        .map(record -> record.get(0).asRelationship())
                        .doOnSubscribe(it -> LOG.info("Start reading relationships")))
                , ReactiveSession::close);
    }

    private Supplier<ReactiveSession> getRxSession() {
        return () -> driver.session(ReactiveSession.class, SessionConfig.forDatabase(databaseName));
    }

    public long getTotalNodeCount() {
        try (var session = driver.session(SessionConfig.forDatabase(databaseName))) {
            return session.run("MATCH (n) RETURN count(n) AS count").single().get("count").asLong();
        }
    }

    public long getTotalRelationshipCount() {
        try (var session = driver.session(SessionConfig.forDatabase(databaseName))) {
            return session.run("MATCH ()-[r]->() RETURN count(r) AS count").single().get("count").asLong();
        }
    }
}
