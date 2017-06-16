package com.jonny.pcfpgrowth.parallelPCFPgrowth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.jonny.pcfpgrowth.Parallel;
import com.jonny.pcfpgrowth.util.MISTree;
import com.jonny.pcfpgrowth.util.Pair;
import com.jonny.pcfpgrowth.util.Parameters;


/**
*@author created by jonny
*@date 2017年5月15日--上午10:05:03
*
**/
public class ParallelPCFPGrowthMapper extends Mapper<LongWritable, Text, IntWritable, MISTree>
{
	private Pattern splitter;
	//其中item作为fMap的键，item在flist中的位置序号作为fMap的值
	//这样做的原因是之后将fMap分Q个group时需要用到这个位置序号。
	Map<String, Integer> fMap = new HashMap<>();
	List<Pair<String, Long>> itemHeaderTable = new ArrayList<>();	

	private int maxPerGroup;
	
	private final IntWritable wGroupID = new IntWritable();
	
	/**
	 * 在map方法中，输入是字节偏移量和事务数据库中的某一行数据。
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, MISTree>.Context context)
			throws IOException, InterruptedException
	{
		//
		String[] items = splitter.split(value.toString());
		//为了过滤非频繁项，通过fMap.containsKey(item)方法来查找该项是否存在于fList中。
		HashSet<Integer> itemSet = new HashSet<>();
		for(String item : items)
		{
			//删除事务中的非频繁item
			if (fMap.containsKey(item) && !item.trim().isEmpty())
			{
				itemSet.add(fMap.get(item));
			}
		}
		//itemArr复制itemSet中的数据，并按照位置序号递增进行排序，即按照支持度递减进行排序。
		List<Integer> itemArr = new ArrayList<>(itemSet.size());
		//记录item索引的List
		itemArr.addAll(itemSet);
		//按照索引排序 及 按照最小支持度从大到小排序
		Collections.sort(itemArr);
		
		//将一条事务分组
		HashSet<Integer> groups = new HashSet<>();
		/*
		 * for循环从itemArr的最后一个元素向前遍历，
		 * 如果其所对应的groupID不在groups中，那么将初始化TransactionTree，
		 * 将itemArr[0]，itemArr[1]，…，itemArr[j]存入该TransactionTree中。
		 * groupID的计算非常简单，将位置序号除以maxPerGroup即可。
		 */
		for(int j = itemArr.size() -1; j >= 0; j--)
		{
			int item = itemArr.get(j);
			int groupID = Parallel.getGroup(item, maxPerGroup);
			
			if (!groups.contains(groupID))
			{
				ArrayList<Integer> tempItems = new ArrayList<>();
				for(int i = 0; i <= j; i++)
				{
					tempItems.add(itemArr.get(i));
				}
				context.setStatus("Parallel FPGrowth: Generating Group Dependent transactions for: "
                        + item);
				wGroupID.set(groupID);
				context.write(wGroupID, new MISTree(tempItems));
			}
			groups.add(groupID);
		}
	}
	
	
	@Override
	protected void setup(Mapper<LongWritable, Text, IntWritable, MISTree>.Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		super.setup(context);
		
		
		itemHeaderTable = Parallel.readFList(context.getConfiguration());
		fMap = Parallel.getfMap(itemHeaderTable);
		
		
		Parameters params = new Parameters(context.getConfiguration().get(
				Parallel.PCFP_PARAMETERS,""));
		
		splitter = Pattern.compile(params.get(Parallel.SPLIT_PATTERN,Parallel.SPLITTER.toString()));
		
		maxPerGroup = params.getInt(Parallel.MAX_PER_GROUP, 0);
	}
}
