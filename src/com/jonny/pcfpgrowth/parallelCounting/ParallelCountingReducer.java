package com.jonny.pcfpgrowth.parallelCounting;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
*@author created by jonny
*@date 2017年5月10日--下午4:33:40
*
**/
public class ParallelCountingReducer
 extends Reducer<Text, LongWritable, Text, LongWritable>
{

	@Override
	protected void reduce(Text key, Iterable<LongWritable> values,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException
	{
		long sum = 0;
		for(LongWritable value : values)
		{
			sum += value.get();
		}
		context.write(key, new LongWritable(sum));
	}
}
