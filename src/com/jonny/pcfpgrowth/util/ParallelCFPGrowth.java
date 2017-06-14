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
*@date 2017年5月16日--下午3:30:40
*
**/
public class ParallelCFPGrowth
{


	Long startTime;
	Long endTime;
	private static final Logger log = LoggerFactory.getLogger(ParallelCFPGrowth.class);
	
	//存放 item所对应的index 及 其 MIS
	Map<Integer, Long> fSupportMap = new HashMap<>();
	// 存放item 和其对应的index
	Map< String ,Integer> fMap = new HashMap<>();
	// 存放 index 和其对应的item
	Map<Integer, String> map = new HashMap<>();
	// 存放 index 和其 对应的最小支持度
	Map<Integer, Long> supportMap = new HashMap<>();
	
	Long MIS[] ;

	//存放 频繁项集 和其 支持度
	List<Pair<List<String>, Long>> patterns = new ArrayList<>();
	 /**
	  * 
	  * @param tree 根据每组事务构建的树
	  * @param mapSupport 该组中 每个item 的 支持度
	  * @param transcationCount 该组中的事务数量
	  * @param itemHeaderTable 该数据集中的 每个item 和其对应的最小支持度
	  * @param leastMIS 该数据集挖掘频繁项集时 的最小支持度阈值
	  * @return 返回 频繁项集 及其支持度
	  */
	public TopKStringPatterns runPCFPGrowth(MTree tree,
								Map<Integer, Long> mapSupport, 
								Long transcationCount,
								List<Pair<String, Long>> itemHeaderTable, 
								long leastMIS)
	{
		
		startTime  = System.currentTimeMillis();
		
		tree.createHeaderList();
		// 创建 树的 headerList 表
//		tree.headerList = new ArrayList<>(mapSupport.keySet());
		if (mapSupport.keySet().size() != tree.headerList.size())
		{
			
			throw new IllegalStateException("该组中的item 统计错误， 导致item数量" 
										+ mapSupport.keySet().size() + 
										" 和建树后的item不一致" + tree.headerList.size()
										);
		}
		
		//从 itemHeaderTable 中获得 该树中 每个item 的 MIS 并存放在一个map中
		initMIS(itemHeaderTable, tree);
		//在 mapper 过程中已经删除了不满足leastMIS的项 
		// merge child node with the same item id
		tree.MISMerge(tree.root);
		
		//  We start to mine the FP-Tree by calling the recursive method.
		// Initially, prefix alpha is empty.
		//前缀
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
			//如果该节点没有 
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
		// reverse order从headerList的最后一个节点开始
		for(int i = tree.headerList.size() - 1; i >= 0; i--)
		{
			//该节点的index
			Integer item = tree.headerList.get(i);
			//该节点在该组中的支持度
			long support = mapSupport.get(item);
			//如果 找不到该节点的最小支持度报错
			if (fSupportMap.get(item) == null)
			{
				throw new IllegalStateException("Null Item");
			}
			
			// 设置准备挖掘的频繁项集的最小支持度 
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
			
			//生成该item节点的所有前缀 包含该节点
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
				//前缀第一个节点的计数
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
		//将item 所对应的编号及 mis 存储到 map中。
		// 构建 item对应的index 
		 fMap = Parallel.getfMap(itemHeaderTable);
		
		 int maxItemId = 0;
		 for(String item : fMap.keySet())
		 {
			 map.put(fMap.get(item), item);
		 }
		 //每个item对应的index 及 其 最小支持度
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

		log.info("该组的item 一共有: " + fSupportMap.keySet().size());
		
		if (fSupportMap.keySet().size() != tree.headerList.size())
		{
			throw new IllegalStateException("ERROR!!" + fSupportMap.keySet().size() + " "  + tree.headerList.size());
		}
		
	}
}
