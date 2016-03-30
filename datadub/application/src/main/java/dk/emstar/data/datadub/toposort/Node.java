package dk.emstar.data.datadub.toposort;

import java.util.Set;

import com.google.common.collect.Sets;

public class Node {
    private final String name;
    private final Set<Edge> inEdges;
    private final Set<Edge> outEdges;

    public Node(String name) {
        this.name = name;
        inEdges = Sets.newHashSet();
        outEdges = Sets.newHashSet();
    }

    public Node addEdge(Node node) {
        Edge edge = new Edge(this, node);
        outEdges.add(edge);
        node.inEdges.add(edge);
        return this;
    }

    public String getName() {
        return name;
    }

    public Set<Edge> getInEdges() {
        return inEdges;
    }

    public Set<Edge> getOutEdges() {
        return outEdges;
    }

    @Override
    public String toString() {
        return name;
    }
}