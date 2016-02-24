package dk.emstar.data.datadub.toposort;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

public class TopologicalSorter {

	// TODO make a unit test
	public static void main(String[] args) {
		Node seven = new Node("7");
		Node five = new Node("5");
		Node three = new Node("3");
		Node eleven = new Node("11");
		Node eight = new Node("8");
		Node two = new Node("2");
		Node nine = new Node("9");
		Node ten = new Node("10");
		seven.addEdge(eleven).addEdge(eight);
		five.addEdge(eleven);
		three.addEdge(eight).addEdge(ten);
		eleven.addEdge(two).addEdge(nine).addEdge(ten);
		eight.addEdge(nine).addEdge(ten);

		
		TopologicalSorter topologicalSorter = new TopologicalSorter();
		List<Node> allNodes = Lists.newArrayList(seven, five, three, eleven, eight, two, nine, ten);
		List<Node> solution = topologicalSorter.toposort(allNodes);
		
		
		// Check to see if all edges are removed
		boolean cycle = topologicalSorter.hasCycle(allNodes);
		if (cycle) {
			System.out.println("Cycle present, topological sort not possible");
		} else {
			System.out.println("Topological Sort: " + solution);
		}
	}

	
	public List<Node> toposort(List<Node> nodes) {
		List<Node> result = Lists.newArrayList();

		Set<Node> workingQueue = nodes.stream()
			.filter(o -> o.getInEdges().isEmpty())
			.collect(Collectors.toSet());

		while (!workingQueue.isEmpty()) {
			Node currentNode = workingQueue.iterator().next();
			workingQueue.remove(currentNode);

			result.add(currentNode);

			for (Iterator<Edge> iterator = currentNode.getOutEdges().iterator(); iterator.hasNext();) {
				Edge outEdge = iterator.next();
				Node toNode = outEdge.getTo();
				iterator.remove();
				toNode.getInEdges().remove(outEdge);

				if (toNode.getInEdges().isEmpty()) {
					workingQueue.add(toNode);
				}
			}
		}
		return result;
	}

	private boolean hasCycle(List<Node> allNodes) {
		for (Node n : allNodes) {
			if (!n.getInEdges().isEmpty()) {
				return true;
			}
		}
		return false;
	}
}