package com.jonny.pcfpgrowth.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
*@author created by jonny
*@date 2017年6月7日--上午11:20:27
*
**/
public class MTree
{

//	//headertable  按照最小支持度降序存储 item 和 MIS
	List<Pair<String, Long>> itemHeaderTable = null;
	//保存headertable 中 每个item 在 MISTree中的第一个节点连接  
	private Map<Integer, MISNode> mapItemFirstNode = new HashMap<>();
	
	//保存每个item 在MISTree中的最后一个节点连接
	private Map<Integer, MISNode> mapItemLastNode = new HashMap<>();
	
//	 Map<Integer, Long> mapSupport = new HashMap<>();
	
	List<Integer> headerList = new ArrayList<>();
	
	private Set<Integer> headerListSet = new HashSet();
	
	MISNode root = new MISNode();
	
	public void addTree(List<Integer> transactionSet)
	{
		MISNode currentNode = root;
		
		for (Integer item : transactionSet)
		{
			
			
			if (! headerListSet.contains(item))
			{
				headerListSet.add(item);
			}
			MISNode child = currentNode.getChildWithID(item);
			if (child == null)
			{
				MISNode node = new MISNode();
				node.itemID = item;
				node.parent = currentNode;
				currentNode.childs.add(node);
//				MISNode node = new MISNode(item, currentNode);
				currentNode = node;
				fixNodeLinks(item, node);
			}else {
				child.counter++;
				currentNode = child;
			}
		}
	}

	private void fixNodeLinks(Integer item, MISNode node)
	{
		// TODO Auto-generated method stub
		MISNode lastNode = mapItemLastNode.get(item);
		if (lastNode != null)
		{
			lastNode.nodeLink = node;
		}
		
		mapItemLastNode.put(item, node);
		
		MISNode headernode = mapItemFirstNode.get(item);
		if (headernode == null)
		{
			mapItemFirstNode.put(item, node);
		}
		
		
	}
	
	public void createHeaderList()
	{
		headerList = new ArrayList<>(headerListSet);
		Collections.sort(headerList);
	}
	
//	public List<Integer> getHeaderList()
//	{
//		createHeaderList();
//		return headerList;
//	}
	
	public Map<Integer, MISNode> getMapItemFirstNode()
	{
		return mapItemFirstNode;
	}
	
	public Map<Integer, MISNode> getMapItemLastNode()
	{
		return mapItemLastNode;
	}
	
	/**
	 * MIS merge procedure (a recursive method)
	 * @param treeRoot a subtree root
	 */
	void MISMerge(MISNode treeRoot) {
		// stop recursion
		if (treeRoot == null)
			return;

		for(int i=0; i< treeRoot.childs.size(); i++){
			MISNode node1 = treeRoot.childs.get(i);
			for(int j=i+1; j< treeRoot.childs.size(); j++){
				MISNode node2 = treeRoot.childs.get(j);
				
				if (node2.itemID == node1.itemID) {
					// (1) merge node1 and node2 
					node1.counter += node2.counter;
					node1.childs.addAll(node2.childs);
					// remove node 2 from child list
					treeRoot.childs.remove(j);   
					j--;                         
					
					// (2) remove node2 from the header list
					// If node2 is the first item in the header list:
					MISNode headernode = mapItemFirstNode.get(node1.itemID);   
					if(headernode == node2){
						mapItemFirstNode.put(node2.itemID, node2.nodeLink);
					}
					else{// Otherwise, search for node 2 and then remove it
						while (headernode.nodeLink != node2){ 
							headernode = headernode.nodeLink;
						}
						// fix nodelink
						headernode.nodeLink = headernode.nodeLink.nodeLink; 
					}
				}// if
			}
		}
		// for all children, merge their children
		for (MISNode node1 : treeRoot.childs){
			MISMerge(node1);
		}
	}

	public void addPrefixPath(List<MISNode> prefixPath, Map<Integer, Long> mapSupportBeta)
	{
		// TODO Auto-generated method stub
		int pathCount = prefixPath.get(0).counter;
		
		MISNode currentNode = root;
		
		for(int i = prefixPath.size() - 1; i >= 1;i --)
		{
			MISNode pathItem = prefixPath.get(i);
			
//			if (mapSupportBeta.get(pathItem.itemID) < leastMIS)
//			{
//				 continue;
//			}
			
			MISNode child = currentNode.getChildWithID(pathItem.itemID);
			if (child == null)
			{
				MISNode node = new MISNode();
				node.itemID = pathItem.itemID;
				node.parent = currentNode;
				node.counter = pathCount;
				currentNode.childs.add(node);
				
				currentNode = node;
				
				fixNodeLinks(pathItem.itemID, node);
			}else{
				child.counter += pathCount;
				currentNode = child;
			}
		}
	}
}
