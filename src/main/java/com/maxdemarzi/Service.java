package com.maxdemarzi;

import org.apache.commons.lang.mutable.MutableInt;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.schema.Schema;
import org.roaringbitmap.RoaringBitmap;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Path("/v1")
public class Service {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final ReverseMapComparator REVERSE_MAP_COMPARATOR = new ReverseMapComparator();

    @GET
    @Path("/migrate")
    public String migrate(@Context GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            Schema schema = db.schema();
            schema.constraintFor(Labels.User)
                    .assertPropertyIsUnique("username")
                    .create();
            tx.success();
        }
        // Wait for indexes to come online
        try (Transaction tx = db.beginTx()) {
            Schema schema = db.schema();
            schema.awaitIndexesOnline(1, TimeUnit.DAYS);
            tx.success();
        }
        return "Migrated!";
    }

    @GET
    @Path("/recommend/{username}")
    public Response Recommend(@PathParam("username") String username, @Context GraphDatabaseService db) throws IOException {
        List<Map<String, Object>> results = new ArrayList<>();

        Set<Node> likedItems = new HashSet<>();
        Set<Node> dislikedItems = new HashSet<>();
        Set<Node> purchasedItems = new HashSet<>();

        try (Transaction tx = db.beginTx()) {
            final Node user = db.findNode(Labels.User, "username", username);

            if ( user != null) {
                for (Relationship rel : user.getRelationships(RelationshipTypes.LIKES, Direction.OUTGOING)) {
                    likedItems.add(rel.getEndNode());
                }

                for (Relationship rel : user.getRelationships(RelationshipTypes.PURCHASED, Direction.OUTGOING)) {
                    purchasedItems.add(rel.getEndNode());
                }

                for (Relationship rel : user.getRelationships(RelationshipTypes.DISLIKES, Direction.OUTGOING)) {
                    dislikedItems.add(rel.getEndNode());
                }

                // Get up to 25 Similar Users
                HashMap<Node, MutableInt> otherUsers = getOtherUsers(likedItems, purchasedItems, user);
                ArrayList<Node> similarUsers = findTopK(otherUsers.entrySet(), 25);

                HashMap<Node, MutableInt> otherItems = getOtherItems(similarUsers);

                // Remove items I've already purchased, liked, or disliked
                for (Node item : purchasedItems) {
                    otherItems.remove(item);
                }
                for (Node item : likedItems) {
                    otherItems.remove(item);
                }
                for (Node item : dislikedItems) {
                    otherItems.remove(item);
                }

                ArrayList<Node> topItems = findTopK(otherItems.entrySet(), 10);

                for (Node item : topItems) {
                    Map<String, Object> resultsEntry = new HashMap<>();
                    for (String prop : item.getPropertyKeys()) {
                        resultsEntry.put(prop, item.getProperty(prop));
                    }
                    results.add(resultsEntry);
                }
            }
        }
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    @GET
    @Path("/recommend2/{username}")
    public Response Recommend2(@PathParam("username") String username, @Context GraphDatabaseService db) throws IOException {
        List<Map<String, Object>> results = new ArrayList<>();

        Set<Node> likedItems = new HashSet<>();
        Set<Node> dislikedItems = new HashSet<>();
        Set<Node> purchasedItems = new HashSet<>();

        try (Transaction tx = db.beginTx()) {
            final Node user = db.findNode(Labels.User, "username", username);

            if ( user != null) {
                for (Relationship rel : user.getRelationships(RelationshipTypes.LIKES, Direction.OUTGOING)) {
                    likedItems.add(rel.getEndNode());
                }

                for (Relationship rel : user.getRelationships(RelationshipTypes.PURCHASED, Direction.OUTGOING)) {
                    purchasedItems.add(rel.getEndNode());
                }

                RoaringBitmap dislikes = getRoaringBitmap(user, "dislikes");
                for (int nodeId : dislikes.toArray()) {
                    dislikedItems.add(db.getNodeById(nodeId));
                }

                // Get up to 25 Similar Users
                HashMap<Node, MutableInt> otherUsers = getOtherUsers(likedItems, purchasedItems, user);
                ArrayList<Node> similarUsers = findTopK(otherUsers.entrySet(), 25);

                HashMap<Node, MutableInt> otherItems = getOtherItems(similarUsers);

                // Remove items I've already purchased, liked, or disliked
                for (Node item : purchasedItems) {
                    otherItems.remove(item);
                }
                for (Node item : likedItems) {
                    otherItems.remove(item);
                }
                for (Node item : dislikedItems) {
                    otherItems.remove(item);
                }

                ArrayList<Node> topItems = findTopK(otherItems.entrySet(), 10);

                for (Node item : topItems) {
                    Map<String, Object> resultsEntry = new HashMap<>();
                    for (String prop : item.getPropertyKeys()) {
                        resultsEntry.put(prop, item.getProperty(prop));
                    }
                    results.add(resultsEntry);
                }
            }
        }
        return Response.ok().entity(objectMapper.writeValueAsString(results)).build();
    }

    private RoaringBitmap getRoaringBitmap(Node user, String property) throws IOException {
        RoaringBitmap rb = new RoaringBitmap();
        byte[] nodeIds = (byte[])user.getProperty(property);
        ByteArrayInputStream bais = new ByteArrayInputStream(nodeIds);
        rb.deserialize(new DataInputStream(bais));
        return rb;
    }

    private static HashMap<Node, MutableInt> getOtherItems(ArrayList<Node> similarUsers) {
        HashMap<Node, MutableInt> otherItems = new HashMap<>();
        for (Node similarUser : similarUsers) {
            for (Relationship rel : similarUser.getRelationships(Direction.OUTGOING,
                    RelationshipTypes.PURCHASED, RelationshipTypes.LIKES)) {
                Node item = rel.getEndNode();
                MutableInt mutableInt = otherItems.get(item);
                if (mutableInt == null) {
                    otherItems.put(item, new MutableInt(1));
                } else {
                    mutableInt.increment();
                }
            }
        }
        return otherItems;
    }

    private static HashMap<Node, MutableInt> getOtherUsers(Set<Node> likedItems, Set<Node> purchasedItems, Node user) {
        HashMap<Node, MutableInt> otherUsers = new HashMap<>();

        for (Node item : purchasedItems) {
            // Give 5 points to every person who purchased an Item I also purchased
            for (Relationship rel : item.getRelationships(RelationshipTypes.PURCHASED, Direction.INCOMING)) {
                Node otherUser = rel.getStartNode();
                MutableInt mutableInt = otherUsers.get(otherUser);
                if (mutableInt == null) {
                    otherUsers.put(otherUser, new MutableInt(5));
                } else {
                    mutableInt.add(5);
                }
            }
            // Give 3 points to every person who liked an Item I purchased
            for (Relationship rel : item.getRelationships(RelationshipTypes.LIKES, Direction.INCOMING)) {
                Node otherUser = rel.getStartNode();
                MutableInt mutableInt = otherUsers.get(otherUser);
                if (mutableInt == null) {
                    otherUsers.put(otherUser, new MutableInt(3));
                } else {
                    mutableInt.add(3);
                }
            }
        }

        for (Node item : likedItems) {
            // Give 2 points to every person who liked an Item I also liked
            for (Relationship rel : item.getRelationships(RelationshipTypes.LIKES, Direction.INCOMING)) {
                Node otherUser = rel.getStartNode();
                MutableInt mutableInt = otherUsers.get(otherUser);
                if (mutableInt == null) {
                    otherUsers.put(otherUser, new MutableInt(2));
                } else {
                    mutableInt.add(2);
                }
            }
            // Give 1 point to every person who purchased an Item I liked
            for (Relationship rel : item.getRelationships(RelationshipTypes.PURCHASED, Direction.INCOMING)) {
                Node otherUser = rel.getStartNode();
                MutableInt mutableInt = otherUsers.get(otherUser);
                if (mutableInt == null) {
                    otherUsers.put(otherUser, new MutableInt(1));
                } else {
                    mutableInt.increment();
                }
            }
        }
        // Remove self from similar users
        otherUsers.remove(user);
        return otherUsers;
    }

    static ArrayList<Node> findTopK(Collection<Map.Entry<Node, MutableInt>> items, int topK) {
        Map.Entry<Node, MutableInt>[] heap = new Map.Entry[topK];
        int count=0;
        int minValue=Integer.MAX_VALUE;
        for (Map.Entry<Node, MutableInt> item : items) {
            if (count < topK || item.getValue().intValue() > minValue) {
                int idx = Arrays.binarySearch(heap, 0, count, item, REVERSE_MAP_COMPARATOR);
                idx = (idx < 0) ? -idx : idx + 1;
                int length = topK - idx;
                if (length > 0 && idx < topK) System.arraycopy(heap, idx - 1, heap, idx, length);
                heap[idx-1]=item;
                if (count < topK) count++;
                minValue = heap[count-1].getValue().intValue();
            }
        }

        ArrayList<Node> topNodes = new ArrayList<>(topK);
        for (int i = 0; i < Math.min(items.size(), topK); i++){
            topNodes.add(heap[i].getKey());
        }
        return topNodes;
    }

}
