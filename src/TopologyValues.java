import java.util.ArrayList;


public class TopologyValues {
private String ClassName;
private String Grouping;
private int parallelism;
private String NextTaskId;
@Override
public String toString() {
	return "TopologyValues [ClassName=" + ClassName + ", Grouping=" + Grouping
			+ ", parallelism=" + parallelism + ", NextTaskId=" + NextTaskId
			+ ", nodeList=" + nodeList + "]";
}
public String getClassName() {
	return ClassName;
}
public void setClassName(String className) {
	ClassName = className;
}
public String getGrouping() {
	return Grouping;
}
public void setGrouping(String grouping) {
	Grouping = grouping;
}
public int getParallelism() {
	return parallelism;
}
public void setParallelism(int parallelism) {
	this.parallelism = parallelism;
}
public String getNextTaskId() {
	return NextTaskId;
}
public void setNextTaskId(String nextTaskId) {
	NextTaskId = nextTaskId;
}
public ArrayList<String> getNodeList() {
	return nodeList;
}
public void setNodeList(ArrayList<String> nodeList) {
	this.nodeList = nodeList;
}
private ArrayList<String> nodeList;
}
