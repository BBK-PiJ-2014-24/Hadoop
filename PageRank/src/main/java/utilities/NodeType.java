/**
 * Enum Structure for Defining Type of Node. Whether a Node Carrying Prob 
 * Mass or Node's Adjancency List or entire structure. The Constructor is passed byte
 * references to help with serializing the enum. 
 */

package utilities;

public enum NodeType {
	
	
	// Enum List
	// ---------
	Structure((byte) 0), // Node's PageRank Prob Mass + Adjancency List
	ProbMass((byte) 1), // Node's PageRank prob Mass
	AdjList((byte) 2); // Node's Adjancency List
	
	byte val;
	
	private NodeType(byte v){
		this.val = v;
	}	
	
	public byte getNodeTypeCode(){
		return val;
	}

}

