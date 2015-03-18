package com.maxdemarzi;

import org.neo4j.graphdb.RelationshipType;

public enum RelationshipTypes implements RelationshipType {
    LIKES,
    DISLIKES,
    PURCHASED
}
