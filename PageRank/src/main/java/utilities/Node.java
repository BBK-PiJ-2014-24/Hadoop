/**
 * Node Used for PageRank
 */
package utilities;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.ArrayPrimitiveWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;



public class Node implements WritableComparable<Node>{
	
	// Fields
	// ------
	
	private int nodeID;
	private NodeType nodeType;
	private float pageRank;
	private  int[] adjList;
	

	
	// for helping with serialization
	NodeType[] enumSerial = new NodeType[]{NodeType.CompleteStructure, 
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
		
		if(adjList == null || adjList.length == 0)
			this.adjList = arr;
		else{
			int[] combineArr = new int[adjList.length + arr.length];
			System.arraycopy(adjList, 0, combineArr, 0, adjList.length);
			System.arraycopy(arr, 0, combineArr, adjList.length, arr.length);
			adjList = combineArr;
		}
			
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
		
		if(nodeType.equals(NodeType.CompleteStructure)){
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
		
		if(nodeType.equals(NodeType.CompleteStructure)){
			out.writeFloat(pageRank);
		}
		
		ArrayPrimitiveWritable writAdjList = new ArrayPrimitiveWritable();
		writAdjList.set(adjList);
		writAdjList.write(out);
	
	}
	
	@Override
	public String toString(){
		String s = "";
		//s += "NodeType: " + nodeType;
		//s += "\nNodeID: " + nodeID;
		if(nodeType.equals(NodeType.ProbMass)){
			s += "\tPageRank: " + pageRank;
			return s;
		}
		if(nodeType.equals(NodeType.CompleteStructure)){
			s += "\tPageRank: " + round(pageRank,4);
		}
		
		if(adjList.length > 0)
			s += "\tAdjList: " + Arrays.toString(adjList);
		
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
	
	
	//round()
	// ------
	/**
	* Rounds a double
	* @param d - double 
	* @param dp = dec places
	* @return
	*/
		private float round(float num, int dp){
			
			BigDecimal bd = new BigDecimal(num);
			bd = bd.setScale(dp, RoundingMode.HALF_UP);
			return bd.floatValue();
		}
	
	
	
	
}
