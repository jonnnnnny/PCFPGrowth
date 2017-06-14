package com.jonny.pcfpgrowth.parallelCounting;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.jonny.pcfpgrowth.Parallel;
import com.jonny.pcfpgrowth.util.Parameters;

/**
*@author created by jonny
*@date 2017年5月10日--下午4:27:05
*
**/
public class ParallelCountingMapper
extends	Mapper<LongWritable, Text, Text, LongWritable>
{
	private static final LongWritable ONE = new LongWritable(1);
	
	private Pattern splitter;
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException
	{
		// TODO Auto-generated method stub
		super.setup(context);
		Parameters params = new Parameters(context.getConfiguration().
				get(Parallel.PCFP_PARAMETERS));
		splitter = Pattern.compile(params.get(Parallel.SPLIT_PATTERN,
				Parallel.SPLITTER.toString()));
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException
	{
		String[] items = splitter.split(value.toString());
		context.write(new Text("dataset"),ONE);
		for (String item : items)
		{
			if (item.trim().isEmpty())
			{
				continue;
			}
			context.write(new Text(item), ONE);
		}
	}

}
