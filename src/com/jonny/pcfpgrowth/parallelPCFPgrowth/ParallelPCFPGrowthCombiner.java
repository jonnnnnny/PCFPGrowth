package com.jonny.pcfpgrowth.parallelPCFPgrowth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.jonny.pcfpgrowth.util.MISTree;
import com.jonny.pcfpgrowth.util.Pair;


/**
*@author created by jonny
*@date 2017��5��16��--����3:10:14
*
**/
public class ParallelPCFPGrowthCombiner extends Reducer<IntWritable, MISTree, IntWritable, MISTree>
{

	//������������  �ֱ�ѹ��Ϊ MISTree�ṹ
	@Override
	protected void reduce(IntWritable key, Iterable<MISTree> values,
			Reducer<IntWritable, MISTree, IntWritable, MISTree>.Context context) throws IOException, InterruptedException
	{
		MISTree cTree = new MISTree();
		for (MISTree tr : values)
		{
			for(Pair<List<Integer>, Long> p : tr.transactionSet)
			{
				cTree.addPattern(p.getFirst(), p.getSecond());
			}
		}
		context.write(key, cTree);
	}
}
