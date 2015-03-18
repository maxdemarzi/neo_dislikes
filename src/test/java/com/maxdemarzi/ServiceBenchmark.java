package com.maxdemarzi;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.openjdk.jmh.annotations.*;
import org.roaringbitmap.RoaringBitmap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
public class ServiceBenchmark {

    private Service service;
    private GraphDatabaseService db;

    //@Param({"100", "10000", "100000"})
    @Param({"10000"})
    public int userCount;

    @Param({"1000"})
    public int itemCount;

    @Param({"50"})
    public int likesCount;

    @Param({"250"})
    public int dislikesCount;

    @Param({"5"})
    public int purchaseCount;

    @Setup
    public void prepare() throws IOException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        service = new Service();
        service.migrate(db);
        populateDb(db);
    }

    @TearDown
    public void tearDown() {
        db.shutdown();
    }

    private void populateDb(GraphDatabaseService db) throws IOException {
        ArrayList<Node> users = new ArrayList<>();
        ArrayList<Node> items = new ArrayList<>();
        Random rand = new Random();

        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < userCount; i++) {
                users.add(createNode(db, Labels.User.toString(), "username", "user" + String.valueOf(i)));
            }
            tx.success();
        }

        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < (itemCount + 5); i++){
                items.add(createNode(db, Labels.Item.toString(), "name", "thing" + String.valueOf(i)));
            }
            tx.success();
        }
        Transaction tx = db.beginTx();
        try  {
            for (int i = 0; i < userCount; i++){
                Node user =  users.get(i);

                for (int j = 0; j < likesCount; j++) {
                    int randomItem = rand.nextInt(itemCount);
                    user.createRelationshipTo(items.get(randomItem), RelationshipTypes.LIKES);
                }

                for (int j = 0; j < purchaseCount; j++) {
                    int randomItem = rand.nextInt(itemCount);
                    user.createRelationshipTo(items.get(randomItem), RelationshipTypes.PURCHASED);
                }

                RoaringBitmap dislikes = new RoaringBitmap();
                for (int j = 0; j < dislikesCount; j++) {
                    int randomItem = rand.nextInt(itemCount);
                    dislikes.add(((Number)items.get(randomItem).getId()).intValue());
                    user.createRelationshipTo(items.get(randomItem), RelationshipTypes.DISLIKES);
                }
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                dislikes.serialize(new DataOutputStream(baos));
                user.setProperty("dislikes", baos.toByteArray());

                if(i % 100 == 0){
                    tx.success();
                    tx.close();
                    tx = db.beginTx();
                }

            }
        tx.success();
    } finally {
            tx.close();
        }
    }

    private Node createNode(GraphDatabaseService db, String label, String property, String value) {
        Node node = db.createNode(DynamicLabel.label(label));
        node.setProperty(property, value);
        return node;
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 5)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureRecommend() throws IOException {
        service.Recommend("user10", db);
    }

    @Benchmark
    @Warmup(iterations = 10)
    @Measurement(iterations = 5)
    @Fork(1)
    @Threads(4)
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void measureRecommend2() throws IOException {
        service.Recommend2("user10", db);
    }

}
