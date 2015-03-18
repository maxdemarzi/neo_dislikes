package com.maxdemarzi;

import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.roaringbitmap.RoaringBitmap;

import javax.ws.rs.core.Response;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ServiceTest {
    private GraphDatabaseService db;
    private Service service;
    private ObjectMapper objectMapper = new ObjectMapper();

    @Before
    public void setUp() throws IOException {
        db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        populateDb(db);
        service = new Service();
        service.migrate(db);
    }

    private void populateDb(GraphDatabaseService db) throws IOException {
        ArrayList<Node> users = new ArrayList<>();
        ArrayList<Node> items = new ArrayList<>();

        try (Transaction tx = db.beginTx()) {
            for (int i = 0; i < 100; i++){
                users.add(createNode(db, Labels.User.toString(), "username", "user" + String.valueOf(i)));
            }

            for (int i = 0; i < 105; i++){
                items.add(createNode(db, Labels.Item.toString(), "name", "thing" + String.valueOf(i)));
            }

            for (int i = 0; i < 100; i++){
                Node user = users.get(i);
                user.createRelationshipTo(items.get(i), RelationshipTypes.LIKES);
                user.createRelationshipTo(items.get(i + 1), RelationshipTypes.LIKES);
                user.createRelationshipTo(items.get(i + 2), RelationshipTypes.PURCHASED);
                user.createRelationshipTo(items.get(i + 3), RelationshipTypes.PURCHASED);
                user.createRelationshipTo(items.get(i + 4), RelationshipTypes.DISLIKES);
                user.createRelationshipTo(items.get(i + 5), RelationshipTypes.DISLIKES);

                ByteArrayOutputStream baos = new ByteArrayOutputStream();

                RoaringBitmap dislikes = new RoaringBitmap();
                dislikes.add(((Number)items.get(i+4).getId()).intValue());
                dislikes.add(((Number)items.get(i+5).getId()).intValue());

                dislikes.serialize(new DataOutputStream(baos));
                user.setProperty("dislikes", baos.toByteArray());
            }
            tx.success();
        }
    }

    private Node createNode(GraphDatabaseService db, String label, String property, String value) {
        Node node = db.createNode(DynamicLabel.label(label));
        node.setProperty(property, value);
        return node;
    }

    @After
    public void tearDown() throws Exception {
        db.shutdown();
    }

    @Test
    public void shouldGetRecommendation() throws IOException {
        Response response = service.Recommend("user10", db);

        assertEquals(200, response.getStatus());

        ArrayList<HashMap<String, Object>> actual = objectMapper.readValue((String) response.getEntity(), ArrayList.class);

        ArrayList<HashMap<String, Object>> expected = new ArrayList<HashMap<String,Object>>() {{
            add(new HashMap<String, Object>() {{
                put("name", "thing9");
            }});
            add(new HashMap<String, Object>() {{
                put("name", "thing8");
            }});
            add(new HashMap<String, Object>() {{
                put("name", "thing16");
            }});
            add(new HashMap<String, Object>() {{
                put("name", "thing7");
            }});
        }};
        assertEquals(new HashSet(expected), new HashSet(actual));
    }

    @Test
    public void shouldGetRecommendation2() throws IOException {
        Response response = service.Recommend2("user10", db);
        assertEquals(200, response.getStatus());

        ArrayList<HashMap<String, Object>> actual = objectMapper.readValue((String) response.getEntity(), ArrayList.class);

        ArrayList<HashMap<String, Object>> expected = new ArrayList<HashMap<String,Object>>() {{
            add(new HashMap<String, Object>() {{
                put("name", "thing9");
            }});
            add(new HashMap<String, Object>() {{
                put("name", "thing8");
            }});
            add(new HashMap<String, Object>() {{
                put("name", "thing16");
            }});
            add(new HashMap<String, Object>() {{
                put("name", "thing7");
            }});

        }};
        assertEquals(new HashSet(expected), new HashSet(actual));
    }

    @Test
    public void shouldbeSmallSize() throws IOException {
        RoaringBitmap dislikes = new RoaringBitmap();
        dislikes.add(1);
        assertEquals(18, dislikes.serializedSizeInBytes());
        dislikes.add(1000);
        assertEquals(20, dislikes.serializedSizeInBytes());
        dislikes.add(100000000);
        assertEquals(30, dislikes.serializedSizeInBytes());
        int maxUsers = 100000000;
        Random rand = new Random();
        for (int i = 0; i < 1000; i++){
            int randomItem = rand.nextInt(maxUsers);
            dislikes.add(randomItem);
        }
        int sizeInRelationships = 34 * 1000;
        assertTrue(dislikes.serializedSizeInBytes() <= sizeInRelationships);
    }
}
