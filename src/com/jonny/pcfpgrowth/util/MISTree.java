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
*@date 2017��5��15��--����10:07:06
*
**/
public class MISTree implements Writable
{
	
	private static final Logger log = LoggerFactory.getLogger(MISTree.class);
//	//headertable  ������С֧�ֶȽ���洢 item �� MIS
//	List<Pair<String, Long>> itemHeaderTable = null;
	//����headertable �� ÿ��item �� MISTree�еĵ�һ���ڵ�����  
	Map<Integer, MISNode> mapItemNode = new HashMap<>();
	
	//����ÿ��item ��MISTree�е����һ���ڵ�����
	Map<Integer, MISNode> mapItemLastNode = new HashMap<>();

	//�����洢����������
	public List<Pair<List<Integer>, Long>> transactionSet;
	
	private static final int DEFAULT_CHILDREN_INITIAL_SIZE = 2;
	private static final int DEFAULT_INITIAL_SIZE = 8;
	private static final float GROWTH_RATE = 1.5f;
	private static final int ROOTNODEID = 0;
	
	//����item �ı�ţ���ͬ��֧�µ���ͬ���ֵĽڵ㣬����λ�ò�ͬ
	//�ڵ����������
	//�� attribute[i] ��ʾ 
	private int[] attribute;
	//����ڵ���ӽڵ����
	//childCount[i] = j ��ʾ  �ڵ� i ��  j���ӽڵ�
	private int[] childCount;
	//һ����ά���飬����ĳ�ڵ�ĵ�n���ӽڵ���attribute�����е�λ�ñ�š�
	//��¼ÿһ���綨���ĺ��ӽڵ�
	//nodeChildren[i][j] ��ʾ  
	private int[][] nodeChildren;
	//����ڵ��Ƶ���ȼ���
	private long[] nodeCount;
	//TransactionTree�нڵ����
	private int nodes;
	

	
	MISNode root = new MISNode();
	
	//�ж��Ƿ������ ����MIS��
	//true��ʾ��list��ʽչʾ�� false ��ʾ��������ʽչʾ
	private  boolean representedAsList;
	

	/**
	 * 4�����췽��
	 */
	//����Ĭ�ϴ�С��MISTree
	public MISTree()
	{
		// TODO Auto-generated constructor stub
		this(DEFAULT_INITIAL_SIZE);
	}
	
	//��MISTree�������������ݼ�
	public MISTree(List<Pair<List<Integer>, Long>> transactionSet)
	{
		representedAsList = true;
		this.transactionSet = transactionSet;
	}
	
	
	//��MISTree�����һ��������֧�ֶȵ��������ݣ�
	public MISTree(ArrayList<Integer> items, long support)
	{
		representedAsList = true;
		transactionSet = new ArrayList<>();
		transactionSet.add(new Pair<List<Integer>, Long>(items, support));
	}
	
	
	public MISTree(int size)
	{
		//��ʼ������ 
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

	//����MISTree��ͷ�ڵ�
	//�� �� childCount��0��λ����Ϊ0
	//attribute ��0��λ����Ϊ-1
	//nodeCount[0] ��Ϊ0
	
	private void createRootNode()
	{
		// TODO Auto-generated method stub
		//�ӽڵ����Ϊ0
		childCount[nodes] = 0;
		//��ʼroot��� Ϊ-1
		attribute[nodes] = -1;
		//root��֧�ֶ�Ϊ0
		nodeCount[nodes] = 0;
		if (nodeChildren[nodes] == null)
		{
			nodeChildren[nodes] = new int[DEFAULT_CHILDREN_INITIAL_SIZE];
		}
		//�ڵ��������1
		nodes++;
	}

	/**
	 * ����
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
	    //���������� ���ݣ� ���´���һ������ ���� ԭ�������� ������ ��������
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
	 * ��MISTree�����һ����������
	 * @param transaction  ��������
	 * @param addCount  Ҫ���ӵ�֧�ֶȼ���
	 * @return
	 */
	public int addPattern(List<Integer> transaction, Long addCount)
	{
		//��ʱ�ĸ��ڵ�ID
		int temp = ROOTNODEID;
		int ret = 0;
		//
		boolean addCountMode = true;
		
		//�� MISTree�����һ������
		//����Ϊ��MIS�ź����item
		for(int i = 0; i < transaction.size(); i++)
		{
			//��ȡ itemID
			int attributeValue = transaction.get(i);
			int child;
			//����������¿����ҵ���item 
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
	 * ����temp�ڵ����ӽڵ����Ƿ��к�attributeValueͬ���Ľڵ�
	 * ���û�� addCountMode ����Ϊfalse�� ��myList��ʣ��Ľڵ���ӵ��������
	 * ����У���ͨ��addCount���� ����child�ڵ��֧�ֶȡ�
	 * @param nodeID
	 * @param childAttributeValue
	 * @return
	 */
	private int childWithAttribute(int nodeID, int childAttributeValue)
	{
		//��ȡ���ڵ���ӽڵ����
		int length = childCount[nodeID];
		for(int i = 0; i< length; i++)
		{
			if (attribute[nodeChildren[nodeID][i]] == childAttributeValue)
			{
				//��� �ӽڵ��д��� item 
				return nodeChildren[nodeID][i];
			}
		}
		//childAttributeValue��������nodeID���ӽڵ���
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
		//��ȡ MISTree �еĸ��ڵ㣬���������е�item ���뵽MISTree��
		MISNode currentNode = root;
		for(Integer item : transaction)
		{
			//��ȡ itemid ��ͬ�� �ӽڵ�
			MISNode child = currentNode.getChildWithID(item);
					
			//����ӽڵ���û�и�item
			//�½��ӽڵ� ���뵽currentNode��chidls��
			if (child == null)
			{
				//�½��ڵ� �丸�ڵ�ΪcurrentNode  ��������뵽currentNode �ӽڵ��б���
				MISNode node = new MISNode(item);
				node.setParent(currentNode);
				currentNode.addChild(node);
				
				//ͬʱ�����������һ��item
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
			//ÿ��ͬ��item ʹ��MISNode �е�nodelink����
			lastNode.setNodeLink(node);
		}
		mapItemLastNode.put(item, node);
		
		MISNode headerNode = mapItemNode.get(item);
		//����ýڵ�Ϊ ��item�ĵ�һ���ڵ�  ������뵽 ͷ�ڵ�map��
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
