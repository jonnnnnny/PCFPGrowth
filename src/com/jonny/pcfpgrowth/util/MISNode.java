package com.jonny.pcfpgrowth.util;

import java.util.ArrayList;
import java.util.List;

import com.sun.xml.bind.v2.model.core.ID;

/**
*@author created by jonny
*@date 2017年5月15日--上午10:10:50
*
**/
public class MISNode
{

	int itemID = -1;  // item represented by this node
	int counter = 1;  // frequency counter
	
	// link to parent node
	MISNode parent = null; 
	
	// links to child nodes
	List<MISNode> childs = new ArrayList<MISNode>();
	
	 // link to next node with the same item id (for the header table).
	MISNode nodeLink = null;
	
	/**
	 * constructor
	 */
	MISNode(){
		
	}

	/**
	 * Return the immmediate child of this node having a given ID.
	 * If there is no such child, return null;
	 */
	MISNode getChildWithID(int id) {
		// for each child
		for(MISNode child : childs){
			// if the id is found, return the node
			if(child.itemID == id){
				return child;
			}
		}
		return null; // if not found, return null
	}
	
	/**
	 * Return the index of the immmediate child of this node having a given ID.
	 * If there is no such child, return -1;
	 */
	int getChildIndexWithID(int id) {
		int i=0;
		// for each child
		for(MISNode child : childs){
			// if the id is found, return the index
			if(child.itemID == id){
				return i;
			}
			i++;
		}
		return -1; // if not found, return -1
	}
}
