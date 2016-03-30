package dk.emstar.data.datadub.toposort;

public class Edge {
	private final Node from;
	private final Node to;

	public Edge(Node from, Node to) {
		this.from = from;
		this.to = to;
	}
	
	public Node getFrom() {
		return from;
	}

	public Node getTo() {
		return to;
	}

	@Override
	public boolean equals(Object obj) {
		Edge e = (Edge) obj;
		return e.from == from && e.to == to;
	}
}