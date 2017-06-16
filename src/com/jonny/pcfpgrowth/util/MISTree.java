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
*@date 2017年5月15日--上午10:07:06
*
**/
public class MISTree implements Writable
{
	
	private static final Logger log = LoggerFactory.getLogger(MISTree.class);
	//用来存储分组后的事务
	public List<List<Integer>> transactionSet;
	
	//true表示以list形式展示
	private  boolean representedAsList;
	
	public MISTree()
	{
		// TODO Auto-generated constructor stub
	}
	
	public MISTree(List<List<Integer>> transactionSet)
	{
		representedAsList = true;
		this.transactionSet = transactionSet;
	}
	
	
	//向MISTree中添加一条包含其支持度的事务数据，
	public MISTree(ArrayList<Integer> items)
	{
		representedAsList = true;
		transactionSet = new ArrayList<>();
		transactionSet.add(items);
	}

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
			for (List<Integer> transaction : transactionSet)
			{
				
				
				vInt.set(transaction.size());
				vInt.write(out);
				
				List<Integer> items = transaction;
				
				for (Integer item : items)
				{
					vInt.set(item);
					vInt.write(out);
				}
			}
		}
	}
	
	
	

	@Override
	public void readFields(DataInput in) throws IOException
	{
		
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
				vInt.readFields(in);
				int length = vInt.get();

				List<Integer> items = new ArrayList<>();
				for(int j = 0; j < length; j++)
				{
					vInt.readFields(in);
					items.add(vInt.get());
				}
				Collections.sort(items);				
				transactionSet.add(items);
			}
		}

	}

}
