package com.maxdemarzi;

import org.apache.commons.lang.mutable.MutableInt;
import org.neo4j.graphdb.Node;

import java.util.Comparator;
import java.util.Map;

public class ReverseMapComparator<T extends Comparable<T>> implements Comparator<Map.Entry<Node, MutableInt>> {
    public int compare(Map.Entry<Node, MutableInt> a, Map.Entry<Node, MutableInt> b) {
        // Reverse Order
        return b.getValue().compareTo(a.getValue());
    }
}