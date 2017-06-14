package com.jonny.pcfpgrowth.util;



import java.util.ArrayList;
import java.util.Arrays;

import java.util.HashMap;
import java.util.HashSet;

import java.util.List;
import java.util.Map;


import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.yarn.webapp.ResponseInfo.Item;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jonny.pcfpgrowth.Parallel;


/**
*@author created by jonny
*@date 2017��5��16��--����3:30:40
*
**/
public class ParallelCFPGrowth
{


	Long startTime;
	Long endTime;
	private static final Logger log = LoggerFactory.getLogger(ParallelCFPGrowth.class);
	
	//��� item����Ӧ��index �� �� MIS
	Map<Integer, Long> fSupportMap = new HashMap<>();
	// ���item �����Ӧ��index
	Map< String ,Integer> fMap = new HashMap<>();
	// ��� index �����Ӧ��item
	Map<Integer, String> map = new HashMap<>();
	// ��� index ���� ��Ӧ����С֧�ֶ�
	Map<Integer, Long> supportMap = new HashMap<>();
	
	Long MIS[] ;

	//��� Ƶ��� ���� ֧�ֶ�
	List<Pair<List<String>, Long>> patterns = new ArrayList<>();
	 /**
	  * 
	  * @param tree ����ÿ�����񹹽�����
	  * @param mapSupport ������ ÿ��item �� ֧�ֶ�
	  * @param transcationCount �����е���������
	  * @param itemHeaderTable �����ݼ��е� ÿ��item �����Ӧ����С֧�ֶ�
	  * @param leastMIS �����ݼ��ھ�Ƶ���ʱ ����С֧�ֶ���ֵ
	  * @return ���� Ƶ��� ����֧�ֶ�
	  */
	public TopKStringPatterns runPCFPGrowth(MTree tree,
								Map<Integer, Long> mapSupport, 
								Long transcationCount,
								List<Pair<String, Long>> itemHeaderTable, 
								long leastMIS)
	{
		
		startTime  = System.currentTimeMillis();
		
		tree.createHeaderList();
		// ���� ���� headerList ��
//		tree.headerList = new ArrayList<>(mapSupport.keySet());
		if (mapSupport.keySet().size() != tree.headerList.size())
		{
			
			throw new IllegalStateException("�����е�item ͳ�ƴ��� ����item����" 
										+ mapSupport.keySet().size() + 
										" �ͽ������item��һ��" + tree.headerList.size()
										);
		}
		
		//�� itemHeaderTable �л�� ������ ÿ��item �� MIS �������һ��map��
		initMIS(itemHeaderTable, tree);
		//�� mapper �������Ѿ�ɾ���˲�����leastMIS���� 
		// merge child node with the same item id
		tree.MISMerge(tree.root);
		
		//  We start to mine the FP-Tree by calling the recursive method.
		// Initially, prefix alpha is empty.
		//ǰ׺
		int[] prefixAlpha = new int[0];
		
		if (tree.headerList.size() > 0)
		{
			cfpgrowth(tree, prefixAlpha, transcationCount,mapSupport);
		}
		
		
		endTime = System.currentTimeMillis();
		
		log.info("PCFPGrowth run Time : " + (endTime - startTime));
		
		return new TopKStringPatterns(patterns);
		
	}

	/**
	 * 
	 * @param tree
	 * @param prefixAlpha
	 * @param prefixSupport
	 * @param mapSupport
	 */
	private void cfpgrowth(MTree tree, int[] prefixAlpha, Long prefixSupport, Map<Integer, Long> mapSupport)
	{
		// We check if there is only one item in the header table
		if (tree.headerList.size() == 1)
		{
			MISNode node = tree.mapItemFirstNode.get(tree.headerList.get(0));
			//����ýڵ�û�� 
			if (node.nodeLink == null)
			{

//				 if(node.counter >= fSupportMap.get(prefixAlpha[0]))
				{
					writeItemSet(prefixAlpha, node.itemID, node.counter);
				}
			}else {
				cfpgrowthMoreThanOnePath(tree, prefixAlpha, prefixSupport, mapSupport);
			}
		}else {
			cfpgrowthMoreThanOnePath(tree, prefixAlpha, prefixSupport, mapSupport);
		}
	}

	private void cfpgrowthMoreThanOnePath(MTree tree, int[] prefixAlpha, Long prefixSupport,
			Map<Integer, Long> mapSupport)
	{
		// TODO Auto-generated method stub
		// We process each frequent item in the header table list of the tree in
		// reverse order��headerList�����һ���ڵ㿪ʼ
		for(int i = tree.headerList.size() - 1; i >= 0; i--)
		{
			//�ýڵ��index
			Integer item = tree.headerList.get(i);
			//�ýڵ��ڸ����е�֧�ֶ�
			long support = mapSupport.get(item);
			//��� �Ҳ����ýڵ����С֧�ֶȱ���
			if (fSupportMap.get(item) == null)
			{
				throw new IllegalStateException("Null Item");
			}
			
			// ����׼���ھ��Ƶ�������С֧�ֶ� 
			long mis = (prefixAlpha.length == 0) ? fSupportMap.get(item) : fSupportMap.get(prefixAlpha[0]);
			
			if (support < mis)
			{
				continue;
			}
			
			long betaSupport = (prefixSupport < support) ? prefixSupport : support;
			if (support >= mis)
			{
				writeItemSet(prefixAlpha, item, betaSupport);
			}
			
			//���ɸ�item�ڵ������ǰ׺ �����ýڵ�
			List<List<MISNode>> prefixPaths = new ArrayList<>();
			MISNode path = tree.mapItemFirstNode.get(item);
			
			while(path != null)
			{
				if (path.parent.itemID != -1)
				{
					List<MISNode> prefixPath = new ArrayList<>();
					
					prefixPath.add(path);
					
					MISNode parent = path.parent;
					while(parent.itemID != -1)
					{
						prefixPath.add(parent);
						parent = parent.parent;
					}
					prefixPaths.add(prefixPath);
				}
				path = path.nodeLink;
			}
			
			Map<Integer, Long> mapSupportBeta = new HashMap<>();
			
			for(List<MISNode> prefixPath : prefixPaths)
			{
				//ǰ׺��һ���ڵ�ļ���
				long pathCount = prefixPath.get(0).counter;
				
				for(int j = 1; j < prefixPath.size(); j++)
				{
					MISNode node = prefixPath.get(j);
					if (mapSupportBeta.get(node.itemID) == null)
					{
						mapSupportBeta.put(node.itemID, pathCount);
					}else {
						mapSupportBeta.put(node.itemID, 
								mapSupportBeta.get(node.itemID)+ pathCount);
					}
				}
			}
			
			MTree treeBeta = new MTree();
			for(List<MISNode> prefixPath : prefixPaths)
			{
				treeBeta.addPrefixPath(prefixPath, mapSupportBeta);
				
			}
			treeBeta.createHeaderList();
			
			if (treeBeta.root.childs.size() > 0)
			{
				int[] beta = new int[prefixAlpha.length + 1];
				System.arraycopy(prefixAlpha, 0, beta, 0, prefixAlpha.length);
				beta[prefixAlpha.length] = item;
				cfpgrowth(treeBeta, beta, betaSupport, mapSupportBeta);
			}
		}
	}

	private void writeItemSet(int[] itemset, int lastItemID, long counter)
	{
		// TODO Auto-generated method stub
		int[] itemsetWithLastItem = new int[itemset.length + 1];
		System.arraycopy(itemset, 0, itemsetWithLastItem, 0, itemset.length);
		itemsetWithLastItem[itemset.length] = lastItemID;
		
		Arrays.sort(itemsetWithLastItem);
//		ItemSet itemSet = new ItemSet(itemsetWithLastItem);
//		itemSet.setAbsoluteSupport(counter);
////		patterns.addItemSet(itemSet, itemSet.size());
		
		Pair<List<String>, Long> pair ;
		List<String> frequent = new ArrayList<>();
		for (Integer index : itemsetWithLastItem)
		{
			String item = map.get(index);
			frequent.add(item);
		}
		pair = new Pair<List<String>, Long>(frequent, counter);
		patterns.add(pair);
		
	}

	private void initMIS(List<Pair<String, Long>> itemHeaderTable, MTree tree)
	{
		//��item ����Ӧ�ı�ż� mis �洢�� map�С�
		// ���� item��Ӧ��index 
		 fMap = Parallel.getfMap(itemHeaderTable);
		
		 int maxItemId = 0;
		 for(String item : fMap.keySet())
		 {
			 map.put(fMap.get(item), item);
		 }
		 //ÿ��item��Ӧ��index �� �� ��С֧�ֶ�
		for (Pair<String, Long> pair : itemHeaderTable)
		{
			String item = pair.getFirst();
			
			Long mis = pair.getSecond();
			
			supportMap.put(fMap.get(item), mis);
			
		}
		
		for (Integer index : tree.headerList)
		{
			if (index > maxItemId)
			{
				maxItemId = index;
			}
			fSupportMap.put(index, supportMap.get(index));
		}
		
//		MIS = new Long[maxItemId + 1];
//		
//		for(Integer index : fSupportMap.keySet())
//		{
//			MIS[index] = fSupportMap.get(index);
//		}

		log.info("�����item һ����: " + fSupportMap.keySet().size());
		
		if (fSupportMap.keySet().size() != tree.headerList.size())
		{
			throw new IllegalStateException("ERROR!!" + fSupportMap.keySet().size() + " "  + tree.headerList.size());
		}
		
	}
}
