/**
 * Node Used for PageRank
 */
package utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class Node implements WritableComparable<Node>{
	
	private int nodeID;
	private NodeType nodeType;
	private float pageRank;
	private  int[] adjList;
	
	// Fields
	// ------
	
	// for helping with serialization
	NodeType[] enumSerial = new NodeType[]{NodeType.Structure, 
										   NodeType.ProbMass, 
										   NodeType.AdjList};	

	// constructor
	// -----------
	public Node(){
		
	}
	
	// getter/setters
	// --------------
	public void setNodeID(int id){
		this.nodeID = id;
	}
	
	/**
	 *  get NodeID
	 * @return returns IntWritable (as it has to be a key in MR)
	 */
	public int getNodeID(){
		return nodeID;
	}
	
	public void setPageRank(float pr){
		this.pageRank = pr;
	}
	
	public float getPageRank(){
		return this.pageRank;
	}
	
	public void setAdjList(int[] arr){
		this.adjList = arr;
	}
	
	public int[] getAdjList(){
		return this.adjList;
	}
	
	public void setNodeType(NodeType t){
		this.nodeType = t;
	}
	
	public NodeType getNodeType(){
		return nodeType;
	}

	// Serialization/Deserialization
	// -----------------------------	
	
	public void readFields(DataInput in) throws IOException{
		
		
		int enumCode = in.readByte();  // read in nodeType code for enum
		nodeType = enumSerial[enumCode]; 
		nodeID = in.readInt();
		
		
		if(nodeType.equals(NodeType.ProbMass)){ 
			pageRank = in.readFloat();
			return;
		}
		
		if(nodeType.equals(NodeType.Structure)){
			pageRank = in.readFloat();
		}
		
		ArrayPrimitiveWritable writAdjList = new ArrayPrimitiveWritable();
		writAdjList.readFields(in);
		adjList = (int[]) writAdjList.get();
		
	}

	public void write(DataOutput out) throws IOException {
		
		byte enumCode = (byte) nodeType.getNodeTypeCode();
		
		out.writeByte(enumCode);  // NodeType
		out.writeInt(nodeID);
		
		if(nodeType.equals(NodeType.ProbMass)){ 
			out.writeFloat(pageRank);
			return;
		}
		
		if(nodeType.equals(NodeType.Structure)){
			out.writeFloat(pageRank);
		}
		
		ArrayPrimitiveWritable writAdjList = new ArrayPrimitiveWritable();
		writAdjList.set(adjList);
		writAdjList.write(out);
	
	}
	
	@Override
	public String toString(){
		String s = "";
		s += "NodeType: " + nodeType;
		s += "\nNodeID: " + nodeID;
		if(nodeType.equals(NodeType.ProbMass)){
			s += "\nPageRank: " + pageRank;
			return s;
		}
		if(nodeType.equals(NodeType.Structure)){
			s += "\nPageRank: " + pageRank;
		}
		
		if(adjList.length > 0)
			s += "\nAdjList: " + adjList.toString();
		
		return s;
	}
	
	@Override
	public int hashCode(){
		
		return nodeID * (adjList.length + 1);
	}
	
	
	@Override
	public boolean equals(Object o){
		Node other = (Node) o;
		if(this.nodeID == other.getNodeID()) return true;
		return false;
	}


	
	//@Override
	public int compareTo(Node n){
		return this.nodeID - n.getNodeID(); 
	}
	
	
	
	
}
