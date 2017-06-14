package com.jonny.pcfpgrowth.parallelPCFPgrowth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jonny.pcfpgrowth.PCFPGrowth;
import com.jonny.pcfpgrowth.Parallel;
import com.jonny.pcfpgrowth.util.CountDescendingPairComparator;
import com.jonny.pcfpgrowth.util.MISTree;
import com.jonny.pcfpgrowth.util.MTree;
import com.jonny.pcfpgrowth.util.ParallelCFPGrowth;
import com.jonny.pcfpgrowth.util.Pair;
import com.jonny.pcfpgrowth.util.Parameters;
import com.jonny.pcfpgrowth.util.TopKStringPatterns;

import sun.util.logging.resources.logging;


/**
*@author created by jonny
*@date 2017年5月16日--下午3:15:48
*
**/
public class ParallelPCFPGrowthReduce extends 
		Reducer<IntWritable, MISTree, Text, TopKStringPatterns>
{

	private static final Logger log = LoggerFactory.getLogger(ParallelPCFPGrowthReduce.class);
	
//	private final List<String> itemReverseMap = new ArrayList<>();
//	private final ArrayList<Long> freqList = new ArrayList<>();
	private int maxHeapSize;
	private Double leastSupport;
	private int numFeatures;
	private int maxPerGroup;
	private long leastMIS;
	
	List<Pair<String,Long>> itemHeaderTable;
	Map<String, Integer> fMap = new HashMap<>();
	Map<Integer, Long> mapSupport = new HashMap<>();
		
	@Override
	protected void reduce(IntWritable key, Iterable<MISTree> values,
			Reducer<IntWritable, MISTree, Text, TopKStringPatterns>.Context context)
			throws IOException, InterruptedException
	{
		MTree tree = new MTree();
		long transcationCount = 0l;
		
		//同一组的事务 构建 MISTree
		for (MISTree mtree : values)
		{
			for(Pair<List<Integer>, Long> transaction : mtree.transactionSet)
			{
				Collections.sort(transaction.getFirst());
				//将分组后的事务建树
				tree.addTransaction(transaction.getFirst());
				// 同时记录 该组中 事务的数量
				transcationCount += transaction.getSecond();
				// 统计 该组中 每个item 支持度
				for (Integer item : transaction.getFirst())
				{
					// increase the support of the item by 1
					if (mapSupport.containsKey(item))
					{
						mapSupport.put(item, mapSupport.get(item)+1l);
					}else{
						mapSupport.put(item, 1l);
					}
//					Long count = mapSupport.get(item);
//					if (count == null) {
//						mapSupport.put(item, 1l);
//					} else {
//						mapSupport.put(item, ++count);
//					}
				}
			}
		}
		
		
		ParallelCFPGrowth pcfpGrowth = new ParallelCFPGrowth();
		TopKStringPatterns topKStringPatterns = pcfpGrowth.runPCFPGrowth(tree, 
				mapSupport,
				transcationCount, 
				itemHeaderTable, 
				leastMIS);
		
		context.write(new Text(key.toString()), topKStringPatterns);
	
	}
	
	
	
	/**
	 * 从 Configuration 中读取到 params 参数
	 */
	@Override
	protected void setup(Reducer<IntWritable, MISTree, Text, TopKStringPatterns>.Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		super.setup(context);
		
		Parameters params = new Parameters(context.getConfiguration().get(
                Parallel.PCFP_PARAMETERS, ""));
		
		
		maxHeapSize = Integer.valueOf(params.get(Parallel.MAXHEAPSIZE, "50"));
        leastSupport = Double.parseDouble(params.get(Parallel.LEAST_SUPPORT));
        log.info("LeastSupport : " + leastSupport);
        maxPerGroup = params.getInt(Parallel.MAX_PER_GROUP, 0);
//        numFeatures = itemReverseMap.size();
		leastMIS =Long.parseLong( params.get(Parallel.LEAST_MIS));
		log.info("leastMIS : " + leastMIS);
        itemHeaderTable = Parallel.readFList(context.getConfiguration());
        fMap = Parallel.getfMap(itemHeaderTable);
	}
}
