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
*@date 2017��5��15��--����10:05:03
*
**/
public class ParallelPCFPGrowthMapper extends Mapper<LongWritable, Text, IntWritable, MISTree>
{
	private Pattern splitter;
	//����item��ΪfMap�ļ���item��flist�е�λ�������ΪfMap��ֵ
	//��������ԭ����֮��fMap��Q��groupʱ��Ҫ�õ����λ����š�
	Map<String, Integer> fMap = new HashMap<>();
	List<Pair<String, Long>> itemHeaderTable = new ArrayList<>();	

	private int maxPerGroup;
	
	private final IntWritable wGroupID = new IntWritable();
	
	/**
	 * ��map�����У��������ֽ�ƫ�������������ݿ��е�ĳһ�����ݡ�
	 */
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, MISTree>.Context context)
			throws IOException, InterruptedException
	{
		//
		String[] items = splitter.split(value.toString());
		//Ϊ�˹��˷�Ƶ���ͨ��fMap.containsKey(item)���������Ҹ����Ƿ������fList�С�
		HashSet<Integer> itemSet = new HashSet<>();
		for(String item : items)
		{
			//ɾ�������еķ�Ƶ��item
			if (fMap.containsKey(item) && !item.trim().isEmpty())
			{
				itemSet.add(fMap.get(item));
			}
		}
		//itemArr����itemSet�е����ݣ�������λ����ŵ����������򣬼�����֧�ֶȵݼ���������
		List<Integer> itemArr = new ArrayList<>(itemSet.size());
		//��¼item������List
		itemArr.addAll(itemSet);
		//������������ �� ������С֧�ֶȴӴ�С����
		Collections.sort(itemArr);
		
		//��һ���������
		HashSet<Integer> groups = new HashSet<>();
		/*
		 * forѭ����itemArr�����һ��Ԫ����ǰ������
		 * ���������Ӧ��groupID����groups�У���ô����ʼ��TransactionTree��
		 * ��itemArr[0]��itemArr[1]������itemArr[j]�����TransactionTree�С�
		 * groupID�ļ���ǳ��򵥣���λ����ų���maxPerGroup���ɡ�
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
