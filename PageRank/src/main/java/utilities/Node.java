/**
 * Node Used for PageRank
 */
package utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Node implements Writable{
	
	private int nodeID;
	private NodeType nodeType;
	private float pageRank;
	private  IntWritableArray2 adjList;
	
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
	public IntWritable getNodeID(){
		return new IntWritable(this.nodeID);
	}
	
	public void setPageRank(float pr){
		this.pageRank = pr;
	}
	
	public float getPageRank(){
		return this.pageRank;
	}
	
	public void setAdjList(IntWritableArray2 arr){
		this.adjList = arr;
	}
	
	public IntWritableArray2 getAdjList(){
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
		
		adjList.readin(in);
		
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
		
		adjList.write(out);
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
		
		if(adjList.length() > 0)
			s += "\nAdjList: " + adjList.toString();
		
		return s;
	}
}
