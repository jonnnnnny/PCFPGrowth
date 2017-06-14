package com.jonny.pcfpgrowth;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.jonny.pcfpgrowth.parallelCounting.ParallelCountingMapper;
import com.jonny.pcfpgrowth.parallelCounting.ParallelCountingReducer;
import com.jonny.pcfpgrowth.parallelPCFPgrowth.ParallelPCFPGrowthCombiner;
import com.jonny.pcfpgrowth.parallelPCFPgrowth.ParallelPCFPGrowthMapper;
import com.jonny.pcfpgrowth.parallelPCFPgrowth.ParallelPCFPGrowthReduce;
import com.jonny.pcfpgrowth.util.HadoopUtils;
import com.jonny.pcfpgrowth.util.MISTree;
import com.jonny.pcfpgrowth.util.Pair;
import com.jonny.pcfpgrowth.util.Parameters;
import com.jonny.pcfpgrowth.util.TopKStringPatterns;


/**
*@author created by jonny
*@date 2017年5月10日--下午3:57:16
*
**/
public class Parallel
{
	public static final String HEADER_TABLE = "item_header_table";
	public static final String G_LISt = "gList";
	public static final String OUTPUT = "output";
	public static final String INPUT = "input";
	public static final String MIN_SUPPORT_THRESHOD = "minSupportThreshod";
	public static final String LEAST_SUPPORT = "leastSupport";
	public static final String LEAST_MIS = "leastMIS";
	public static final String MINCONFIDENCE = "minConfidence";
	public static final String PCFP_PARAMETERS = "pcfp.parameters";
	public static final String DISC = "discretized";
	public static final String FILE_PATTERN = "part-r-00000";
	public static final String ITEM_COUNT = "item_count";
	public static final String ITEMSETS = "itemsets";
	public static final String PCFPGROWTH = "closed";
	public static final String SPLIT_PATTERN = "splitPattern";
	public static final String TOOL = "PCFPGrowth";
	public static final String ENABLE_DISCRETIZATION = "enableDiscretization";
	public static final String ENABLE_RULES = "enableRules";
	//最大的堆大小，表示topK的item数 ， 默认是50
	public static final String  MAXHEAPSIZE = "mapHeapSize";
	public static final String NUM_GROUPS = "numGroups";
	public static final String MAX_PER_GROUP = "maxPerGroup";
	public static final int NUM_GROUPS_DEFAULT = 1000;
	public static final Pattern SPLITTER = Pattern
            .compile("[ ,\t]*[,|\t][ ,\t]*");
	
	private static final Logger log = LoggerFactory.getLogger(Parallel.class);
	
	public static int absSupport;

	public static void run(Parameters params) throws ClassNotFoundException, IOException, InterruptedException
	{
		Configuration conf = new Configuration();
		conf.set(
                "io.serializations",
                "org.apache.hadoop.io.serializer.JavaSerialization,"
                        + "org.apache.hadoop.io.serializer.WritableSerialization");
		Integer enableDiscretization = new Integer(
				params.get(ENABLE_DISCRETIZATION));
		Integer enableRules = new Integer(
				params.get(ENABLE_RULES));
//		if (enableDiscretization.compareTo(new Integer(1)) == 0)
//		{
//			
//		}
		//统计item的支持度
		startParallelCounting(params,conf);
		
		//根据统计 计算出每个item的最小支持度，并按MIS降序排列记录到itemHeaderTable表中
		
		List<Pair<String, Long>> itemHeadertable = readFile(params);
		//保存itemHeadTable到HDFS中
		saveFList(itemHeadertable, params, conf);
		
		int numGroups = params.getInt(NUM_GROUPS, NUM_GROUPS_DEFAULT);
		int maxPerGroup = itemHeadertable.size() / numGroups;
		if (itemHeadertable.size() % numGroups != 0)
		{
			maxPerGroup++;
		}
		params.set(MAX_PER_GROUP, Integer.toString(maxPerGroup));

		startParallelPCFPGrowth(params,conf);
	}
	
	public static int getGroup(int itemId, int maxPerGroup)
	{
		return itemId / maxPerGroup;
	}

	private static void startParallelPCFPGrowth(Parameters params, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException
	{		
		// TODO Auto-generated method stub
		conf.set(PCFP_PARAMETERS, params.toString());
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapred,output.compression.type", "BLOCK");
		
		Path input;
		Integer enableDiscretization = new Integer(params.get(ENABLE_DISCRETIZATION));
		if (enableDiscretization.compareTo(new Integer(1)) == 0) {
            input = new Path(params.get(OUTPUT), DISC);
        } else {
            input = new Path(params.get(INPUT));
        }
		
		Job job = Job.getInstance(conf, "PCFP Growth driver running over input " + input);
		job.setJarByClass(Parallel.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MISTree.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TopKStringPatterns.class);
		
		FileInputFormat.addInputPath(job, input);
		Path outPath = new Path(params.get(OUTPUT), PCFPGROWTH);
		FileOutputFormat.setOutputPath(job, outPath);
		
		HadoopUtils.delete(conf, outPath);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setMapperClass(ParallelPCFPGrowthMapper.class);
//		job.setCombinerClass(ParallelPCFPGrowthCombiner.class);
		job.setReducerClass(ParallelPCFPGrowthReduce.class);
		
		boolean succeeded = job.waitForCompletion(true);
		if (!succeeded)
		{
			throw new IllegalStateException("Job Failed!");
		}
	}

	private static void saveFList(List<Pair<String, Long>> itemHeadertable, Parameters params, Configuration conf) throws IOException
	{
		// TODO Auto-generated method stub
		//itemHeaderTable保存路径
		Path filePath = new Path(params.get(OUTPUT),HEADER_TABLE);
		
		FileSystem fs = FileSystem.get(filePath.toUri(),conf);
		filePath = fs.makeQualified(filePath);
		HadoopUtils.delete(conf, filePath);
		
		FSDataOutputStream outputStream = null;
		try
		{
			outputStream = fs.create(filePath);
			for(Pair<String, Long> pair : itemHeadertable)
			{
				String item = pair.getFirst();
				Long mis = pair.getSecond();
				String string = item + "\t" + mis + "\n";
				outputStream.write(string.getBytes("utf-8"));
				outputStream.flush();
			}
		}
		finally
		{
			// TODO: handle finally clause
			outputStream.close();
		}
//		SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, filePath, Text.class, LongWritable.class);
//		
//		try
//		{
//			for(Pair<String, Long> pair : itemHeadertable)
//			{
//				writer.append(new Text(pair.getFirst()), new LongWritable(pair.getSecond()));
//			}
//		}
//		finally
//		{
//			// TODO: handle finally clause
//			writer.close();
//		}
//		
		DistributedCache.addCacheFile(filePath.toUri(), conf);
		
		
	}

	private static List<Pair<String, Long>> readFile(Parameters params)
	{
		Configuration conf = new Configuration();
		
		Path parallelCountingPath = new Path(params.get(OUTPUT),ITEM_COUNT);
		
		//排序规则 首先比较支持度的大小，如果支持度大小相同 则按照item 的字母顺序排
		PriorityQueue<Pair<String, Long>> queue = new 
				PriorityQueue<>(
						 new Comparator<Pair<String, Long>>() {
							@Override
							public int compare(Pair<String, Long> o1, Pair<String, Long> o2)
							{
								int ret = o2.getSecond().compareTo(o1.getSecond());
								if (ret != 0)
								{
									return ret;
								}
								return o1.getFirst().compareTo(o2.getFirst());
							}
						});
		Long numTrans = null;
		Long leastMis =null;
		FSDataInputStream fsr = null;
		BufferedReader bufferedReader = null;		
		String line = null;		
		try
		{
			FileSystem fs = FileSystem.get(URI.create(params.get(OUTPUT) +"/"+ITEM_COUNT + "/" + FILE_PATTERN),conf);
			fsr = fs.open(new Path(parallelCountingPath, FILE_PATTERN));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			
			while((line = bufferedReader.readLine()) != null)
			{
				//都去出数据集总的记录数
				String[] strs = line.split("\t");
				if (strs[0].equals("dataset"))
				{
					numTrans = Long.parseLong(strs[1]);
					leastMis = (long) (numTrans * Double.parseDouble(params.get(LEAST_SUPPORT)));
					break;
				}
			}
			params.set(LEAST_MIS, leastMis.toString());
//			bufferedReader.reset();	
			//从新打开文件流 从头读取 记录 并计算 每个item的最小支持度
			fsr = fs.open(new Path(parallelCountingPath, FILE_PATTERN));
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			while((line = bufferedReader.readLine()) != null)
			{
				String[] strs = line.split("\t");
				if (!strs[0].equals("dataset"))
				{
					Long mis = (long) (Long.parseLong(strs[1]) * Double.parseDouble(params.get(MIN_SUPPORT_THRESHOD)));
					if (mis >= leastMis)
					{
						Pair<String, Long> item = new Pair<String, Long>(strs[0],mis);
						queue.add(item);					
					}
				}
			}
		}
		catch (IOException e)
		{
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		finally
		{
			// TODO: handle finally clause
			try
			{
				bufferedReader.close();
				
			}
			catch (IOException e)
			{
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
//		for(Pair<Text, LongWritable> record : new )
		List<Pair<String, Long>> fList = Lists.newArrayList();
		while(!queue.isEmpty())
		{
			fList.add(queue.poll());
		}
		return fList;
	}

	public static void startParallelCounting(Parameters params, Configuration conf) throws IOException, ClassNotFoundException, InterruptedException
	{
		conf.set(PCFP_PARAMETERS, params.toString());
		
		conf.set("mapreduce.map.output.compress", "true");
		conf.set("mapred,output.compression.type", "BLOCK");
		
		Path input;
        Integer enableDiscretization = new Integer(
                params.get(ENABLE_DISCRETIZATION));
        if (enableDiscretization.compareTo(new Integer(1)) == 0) {
            input = new Path(params.get(OUTPUT), DISC);
        } else {
            input = new Path(params.get(INPUT));
        }
        
        Job job = Job.getInstance(conf, "Parallel Counting driver running over input: "
                + input);
        job.setJarByClass(Parallel.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        FileInputFormat.addInputPath(job, input);
        Path outPath = new Path(params.get(OUTPUT), ITEM_COUNT);
        FileOutputFormat.setOutputPath(job, outPath);
        
        HadoopUtils.delete(conf,outPath);
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ParallelCountingMapper.class);
//        job.setCombinerClass(ParallelCountingReducer.class);
        job.setReducerClass(ParallelCountingReducer.class);
//        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        
        boolean succeeded = job.waitForCompletion(true);
        if (!succeeded) {
            throw new IllegalStateException("Job failed!");
        }
	}

	//从itemHeaderTable文件中 读取 内容，并生成 headerTable表 格式  
	//Pair<String, Long> String 表示item， Long表示item的最小支持度
	public static List<Pair<String, Long>> readFList(Configuration conf) throws IOException
	{
		// TODO Auto-generated method stub
		List<Pair<String, Long>> headerTable  = new ArrayList<>();
		//获取缓存中的itemHeaderTable文件
		Path[] files = DistributedCache.getLocalCacheFiles(conf);
		if (files == null) {
            throw new IOException(
                    "Cannot read Frequency list from Distributed Cache");
        }
        if (files.length != 1) {
            throw new IOException(
                    "Cannot read Frequency list from Distributed Cache ("
                            + files.length + ')');
        }
		
        FileSystem fs = FileSystem.getLocal(conf);
        Path fListLocalPath = fs.makeQualified(files[0]);
        
        //HDFS上的本地文件不存在
        if (!fs.exists(fListLocalPath))
		{
			URI[] filesURIs = DistributedCache.getCacheFiles(conf);
			if (filesURIs == null)
			{
				throw new IOException(
                        "Cannot read header table from Distributed Cache");
			}
			if (filesURIs.length != 1) 
			{
                throw new IOException(
                        "Cannot read header table from Distributed Cache ("
                                + files.length + ')');
            }
			fListLocalPath = new Path(filesURIs[0].getPath());
		}
        //读取文件并存储在List中
        FSDataInputStream fsr = null;
        BufferedReader bufferedReader = null;
        String line;
        try
		{
			fsr = fs.open(fListLocalPath);
			bufferedReader = new BufferedReader(new InputStreamReader(fsr));
			while((line = bufferedReader.readLine()) != null)
			{
				String[] strs = line.split("\t");
				String item = strs[0];
				Long count = Long.parseLong(strs[1]);
				Pair<String, Long> record = new Pair<String, Long>(
						item, count);
				headerTable.add(record);
				
			}
		}
		finally
		{
			// TODO: handle finally clause
			bufferedReader.close();
			fsr.close();
		}
		
		return headerTable;
	}
	
	public static Map<String, Integer> getfMap(List<Pair<String, Long>> itemHeaderTable)
	{
		int i = 0;
		Map< String,Integer> fMap = new HashMap<>();
		for(Pair<String, Long> e : itemHeaderTable)
		{
			//将itemHeaderTable 中的item 按最小支持度顺序添加索引
			fMap.put(e.getFirst(),i++);
		}
		
		return fMap;
	}
}
