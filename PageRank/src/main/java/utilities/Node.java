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
	private NodeType type;
	private float pageRank;
	private  IntWritableArray adjList;
	
	

	// constructor
	// -----------
	public Node(){
		
	}
	
	// getter/setters
	// --------------
	public void setNodeID(int id){
		this.nodeID = id;
	}
	
	public int getNodeID(){
		return this.nodeID;
	}
	
	public void setPageRank(float pr){
		this.pageRank = pr;
	}
	
	public float getPageRank(){
		return this.pageRank;
	}
	
	public void setAdjList(IntWritableArray arr){
		this.adjList = arr;
	}
	
	public IntWritableArray getAdjList(){
		return this.adjList;
	}
	
	public void setNodeType(NodeType t){
		this.type = t;
	}
	
	public NodeType getNodeType(){
		return type;
	}


	
	public void readFields(DataInput in) throws IOException{
		this.adjList = new IntWritableArray();
		adjList.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}
	

}
