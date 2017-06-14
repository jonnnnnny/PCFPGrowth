package com.jonny.pcfpgrowth.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.xml.bind.v2.runtime.unmarshaller.XsiNilLoader.Array;



/**
*@author created by jonny
*@date 2017年5月15日--上午10:07:06
*
**/
public class MISTree implements Writable
{
	
	private static final Logger log = LoggerFactory.getLogger(MISTree.class);
//	//headertable  按照最小支持度降序存储 item 和 MIS
//	List<Pair<String, Long>> itemHeaderTable = null;
	//保存headertable 中 每个item 在 MISTree中的第一个节点连接  
	Map<Integer, MISNode> mapItemNode = new HashMap<>();
	
	//保存每个item 在MISTree中的最后一个节点连接
	Map<Integer, MISNode> mapItemLastNode = new HashMap<>();

	//用来存储分组后的事务
	public List<Pair<List<Integer>, Long>> transactionSet;
	
	private static final int DEFAULT_CHILDREN_INITIAL_SIZE = 2;
	private static final int DEFAULT_INITIAL_SIZE = 8;
	private static final float GROWTH_RATE = 1.5f;
	private static final int ROOTNODEID = 0;
	
	//保存item 的编号，不同分支下的相同名字的节点，所在位置不同
	//节点的名称属性
	//即 attribute[i] 表示 
	private int[] attribute;
	//保存节点的子节点个数
	//childCount[i] = j 表示  节点 i 有  j个子节点
	private int[] childCount;
	//一个二维数组，保持某节点的第n个子节点在attribute数组中的位置编号。
	//记录每一个界定啊的孩子节点
	//nodeChildren[i][j] 表示  
	private int[][] nodeChildren;
	//保存节点的频繁度计数
	private long[] nodeCount;
	//TransactionTree中节点个数
	private int nodes;
	

	
	MISNode root = new MISNode();
	
	//判断是否进行了 建立MIS树
	//true表示以list形式展示， false 表示以树的形式展示
	private  boolean representedAsList;
	

	/**
	 * 4个构造方法
	 */
	//创建默认大小的MISTree
	public MISTree()
	{
		// TODO Auto-generated constructor stub
		this(DEFAULT_INITIAL_SIZE);
	}
	
	//向MISTree中整个事务数据集
	public MISTree(List<Pair<List<Integer>, Long>> transactionSet)
	{
		representedAsList = true;
		this.transactionSet = transactionSet;
	}
	
	
	//向MISTree中添加一条包含其支持度的事务数据，
	public MISTree(ArrayList<Integer> items, long support)
	{
		representedAsList = true;
		transactionSet = new ArrayList<>();
		transactionSet.add(new Pair<List<Integer>, Long>(items, support));
	}
	
	
	public MISTree(int size)
	{
		//初始化数组 
		// TODO Auto-generated constructor stub
		if (size < DEFAULT_INITIAL_SIZE)
		{
			size = DEFAULT_INITIAL_SIZE;
		}
		childCount = new int[size];
	    attribute = new int[size];
	    nodeCount = new long[size];
	    nodeChildren = new int[size][];
	    createRootNode();
	    representedAsList = false;
	}

	//创建MISTree的头节点
	//即 将 childCount第0个位置设为0
	//attribute 第0个位置设为-1
	//nodeCount[0] 设为0
	
	private void createRootNode()
	{
		// TODO Auto-generated method stub
		//子节点个数为0
		childCount[nodes] = 0;
		//初始root编号 为-1
		attribute[nodes] = -1;
		//root的支持度为0
		nodeCount[nodes] = 0;
		if (nodeChildren[nodes] == null)
		{
			nodeChildren[nodes] = new int[DEFAULT_CHILDREN_INITIAL_SIZE];
		}
		//节点个数增加1
		nodes++;
	}

	/**
	 * 扩容
	 */
	private void resize()
	{
		// TODO Auto-generated method stub
		int size = (int) (GROWTH_RATE * nodes);
		if (size < DEFAULT_INITIAL_SIZE)
		{
			size = DEFAULT_INITIAL_SIZE;
		}
		
		int[] oldChildCount = childCount;
	    int[] oldAttribute = attribute;
	    long[] oldnodeCount = nodeCount;
	    int[][] oldNodeChildren = nodeChildren;
	    //将所有数组 扩容， 从新创建一个数组 并将 原数组数据 拷贝到 新数组中
	    childCount = new int[size];
	    attribute = new int[size];
	    nodeCount = new long[size];
	    nodeChildren = new int[size][];
	    
	    System.arraycopy(oldChildCount, 0, this.childCount, 0, nodes);
	    System.arraycopy(oldAttribute, 0, this.attribute, 0, nodes);
	    System.arraycopy(oldnodeCount, 0, this.nodeCount, 0, nodes);
	    System.arraycopy(oldNodeChildren, 0, this.nodeChildren, 0, nodes);
	}
	

	/**
	 * 向MISTree中添加一组事务数据
	 * @param transaction  事务数据
	 * @param addCount  要增加的支持度计数
	 * @return
	 */
	public int addPattern(List<Integer> transaction, Long addCount)
	{
		//临时的根节点ID
		int temp = ROOTNODEID;
		int ret = 0;
		//
		boolean addCountMode = true;
		
		//向 MISTree中添加一条事务
		//事务为按MIS排好序的item
		for(int i = 0; i < transaction.size(); i++)
		{
			//获取 itemID
			int attributeValue = transaction.get(i);
			int child;
			//如果在子树下可以找到该item 
			if (addCountMode)
			{
				child = childWithAttribute(temp, attributeValue);
				if (child == -1)
				{
					addCountMode = false;
				}else {
					addCount(child, addCount);
					temp = child;
				}
			}
			if (!addCountMode)
			{
				
				child = createNode(temp, attributeValue, addCount);
				temp = child;
				ret ++;
			}
		}
		return ret;
	}
	
	private int createNode(int parentNodeID, int attributeValue, Long count)
	{
		if (nodes >= this.attribute.length)
		{
			resize();
		}
		
		childCount[nodes] = 0;
		this.attribute[nodes] = attributeValue;
		nodeCount[nodes] = count;
		
		if (nodeChildren[nodes] == null)
		{
			nodeChildren[nodes] = new int[DEFAULT_CHILDREN_INITIAL_SIZE];
		}
		
		int childNodeId = nodes++;
		addChild(parentNodeID, childNodeId);
		return childNodeId;
	}

	private void addChild(int parentNodeID, int childNodeId)
	{
		int length = childCount[parentNodeID];
		if (length >= nodeChildren[parentNodeID].length)
		{
			resizeChildren(parentNodeID);
		}
		nodeChildren[parentNodeID][length++] = childNodeId;
		childCount[parentNodeID] = length;
	}

	private void resizeChildren(int parentNodeID)
	{
		int length = childCount[parentNodeID];
		int size = (int) (GROWTH_RATE * length);
		if (size < DEFAULT_CHILDREN_INITIAL_SIZE)
		{
			size = DEFAULT_CHILDREN_INITIAL_SIZE;
		}
		int[] oldNodeChildren = nodeChildren[parentNodeID];
		nodeChildren[parentNodeID] = new int[size];
		System.arraycopy(oldNodeChildren, 0, this.nodeChildren[parentNodeID], 0, length);
	}

	private void addCount(int nodeID, Long nextNodeCount)
	{
		// TODO Auto-generated method stub
		if (nodeID < nodes)
		{
			this.nodeCount[nodeID] += nextNodeCount;
		}
	}

	/**
	 * 返回temp节点下子节点中是否有和attributeValue同名的节点
	 * 如果没有 addCountMode 设置为false， 将myList中剩余的节点添加到这棵树种
	 * 如果有，将通过addCount方法 增加child节点的支持度。
	 * @param nodeID
	 * @param childAttributeValue
	 * @return
	 */
	private int childWithAttribute(int nodeID, int childAttributeValue)
	{
		//获取父节点的子节点个数
		int length = childCount[nodeID];
		for(int i = 0; i< length; i++)
		{
			if (attribute[nodeChildren[nodeID][i]] == childAttributeValue)
			{
				//如果 子节点中存在 item 
				return nodeChildren[nodeID][i];
			}
		}
		//childAttributeValue不存在于nodeID的子节点中
		return -1;
	}
	
	
	public int childCount()
	{
		int sum = 0;
		for(int i = 0; i < nodes; i++)
		{
			sum += childCount[i];
		}
		return sum;
	}
	
	public int childCount(int nodeId)
	{
		return childCount[nodeId];
	}
	
	public long count(int nodeId) {
	    return nodeCount[nodeId];
	  }
	
	public int childAtIndex(int nodeId, int index)
	{
		if (childCount[nodeId] < index)
		{
			return -1;
		}
		return nodeChildren[nodeId][index];
	}
	
	public int attribute(int nodeId)
	{
		return this.attribute[nodeId];
	}
/*
	public void addTransaction(List<Integer> transaction, Long support)
	{
		//获取 MISTree 中的根节点，根据事务中的item 插入到MISTree中
		MISNode currentNode = root;
		for(Integer item : transaction)
		{
			//获取 itemid 相同的 子节点
			MISNode child = currentNode.getChildWithID(item);
					
			//如果子节点中没有该item
			//新建子节点 加入到currentNode的chidls中
			if (child == null)
			{
				//新建节点 其父节点为currentNode  并将其加入到currentNode 子节点列表中
				MISNode node = new MISNode(item);
				node.setParent(currentNode);
				currentNode.addChild(node);
				
				//同时继续事务的先一个item
				currentNode = node;
				
				fixNodeLinks(item, node);
				
			}else {
				long itemsupport = child.getCounter();
				child.setCounter(itemsupport + 1);
				
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
			//每个同名item 使用MISNode 中的nodelink连接
			lastNode.setNodeLink(node);
		}
		mapItemLastNode.put(item, node);
		
		MISNode headerNode = mapItemNode.get(item);
		//如果该节点为 此item的第一个节点  则将其加入到 头节点map中
		if (headerNode == null)
			
		{
			mapItemNode.put(item, node);
		}
	}
*/
	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		out.writeBoolean(representedAsList);
		VIntWritable vInt = new VIntWritable();
		VLongWritable vLong = new VLongWritable();
		
		if (representedAsList)
		{
			int transactionSetSize = transactionSet.size();
			vInt.set(transactionSetSize);
			vInt.write(out);
			for (Pair<List<Integer>, Long> transaction : transactionSet)
			{
				vLong.set(transaction.getSecond());
				vLong.write(out);
				
				vInt.set(transaction.getFirst().size());
				vInt.write(out);
				
				List<Integer> items = transaction.getFirst();
				
				for (Integer item : items)
				{
					vInt.set(item);
					vInt.write(out);
				}
			}
		}else{
			vInt.set(nodes);
			vInt.write(out);
			for (int i = 0; i < nodes; i++)
			{
				vInt.set(attribute[i]);
				vInt.write(out);
				vLong.set(nodeCount[i]);
				vLong.write(out);
				vInt.set(childCount[i]);
				vInt.write(out);
				int max = childCount[i];
				for(int j = 0; j < max; j++)
				{
					vInt.set(nodeChildren[i][j]);
					vInt.write(out);
				}
			}
		}
	}
	
	
	

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		representedAsList = in.readBoolean();
		
		VIntWritable vInt = new VIntWritable();
		VLongWritable vLong = new VLongWritable();
		
		if (representedAsList)
		{
			transactionSet = new ArrayList<>();
			vInt.readFields(in);
			int numTransactions = vInt.get();
			for(int i = 0; i < numTransactions; i++)
			{
				vLong.readFields(in);
				Long support = vLong.get();
				
				vInt.readFields(in);
				int length = vInt.get();
				
//				int[] items = new int[length];
				List<Integer> items = new ArrayList<>();
				for(int j = 0; j < length; j++)
				{
					vInt.readFields(in);
					items.add(vInt.get());
				}
				Pair<List<Integer>, Long> transaction = new Pair<List<Integer>, Long>(items, support);
				transactionSet.add(transaction);
			}
		}else {
			vInt.readFields(in);
		      nodes = vInt.get();
		      attribute = new int[nodes];
		      nodeCount = new long[nodes];
		      childCount = new int[nodes];
		      nodeChildren = new int[nodes][];
		      for (int i = 0; i < nodes; i++) {
		        vInt.readFields(in);
		        attribute[i] = vInt.get();
		        vLong.readFields(in);
		        nodeCount[i] = vLong.get();
		        vInt.readFields(in);
		        int childCountI = vInt.get();
		        childCount[i] = childCountI;
		        nodeChildren[i] = new int[childCountI];
		        for (int j = 0; j < childCountI; j++) {
		          vInt.readFields(in);
		          nodeChildren[i][j] = vInt.get();
		        }
		      }
		}
	}



	
}
