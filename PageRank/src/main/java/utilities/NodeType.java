/**
 * Enum Structure for Defining Type of Node. Whether a Node Carrying Prob 
 * Mass or Node's Structure (i.e. Adjancency List).
 */

package utilities;

public enum NodeType {
	
	
	// Enum List
	// ---------
	Struct_ProbMass((byte) 0), // Node's PageRank Prob Mass + Adjancency List
	ProbMass((byte) 1), // Node's PageRank prob Mass
	Structure((byte) 2); // Node's Adjancency Lsit
	
	byte val;
	
	private NodeType(byte v){
		this.val = v;
	}	

}

