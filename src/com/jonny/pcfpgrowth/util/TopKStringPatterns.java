package com.jonny.pcfpgrowth.util;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
*@author created by jonny
*@date 2017年5月15日--下午2:20:16
*
**/
public class TopKStringPatterns implements Writable
{
	
	private final List<Pair<List<String>, Long>> frequentPatterns;

	public TopKStringPatterns()
	{
		// TODO Auto-generated constructor stub
		frequentPatterns = new ArrayList<>();
	}
	
	public TopKStringPatterns(Collection<Pair<List<String>, Long>> patterns)
	{
		frequentPatterns = new ArrayList<>();
		frequentPatterns.addAll(patterns);
	}
	
	public void add(Pair<List<String>, Long> pair)
	{
		this.frequentPatterns.add(pair);
	}
	
	public Iterator<Pair<List<String>, Long>> iterator()
	{
		return frequentPatterns.iterator();
	}
	
	public List<Pair<List<String>, Long>> getPatterns()
	{
		return frequentPatterns;
	}
	
	public TopKStringPatterns merger(TopKStringPatterns pattern, int heapSize)
	{
		List<Pair<List<String>, Long>> patterns = new ArrayList<>();
		Iterator<Pair<List<String>, Long>> myIterator = frequentPatterns.iterator();
		Iterator<Pair<List<String>, Long>> otherIterator = pattern.iterator();
		Pair<List<String>, Long> myItem = null;
		Pair<List<String>, Long> otherItem = null;
		for(int i = 0; i < heapSize; i++)
		{
			if (myItem == null && myIterator.hasNext())
			{
				myItem = myIterator.next();
			}
			if (otherItem == null && otherIterator.hasNext())
			{
				otherItem = otherIterator.next();
			}
			
			if (myItem != null && otherItem != null)
			{
				int cmp = myItem.getSecond().compareTo(otherItem.getSecond());
				if (cmp == 0)
				{
					cmp = myItem.getFirst().size() - otherItem.getFirst().size();
					if (cmp == 0)
					{
						for(int j = 0; j < myItem.getFirst().size(); j++)
						{
							cmp = myItem.getFirst().get(j).compareTo(otherItem.getFirst().get(j));
							if (cmp != 0)
							{
								break;
							}
						}
					}
				}
				if (cmp <= 0)
				{
					patterns.add(otherItem);
					if (cmp == 0)
					{
						myItem = null;
					}
					otherItem = null;
				}else if (cmp > 0) {
					patterns.add(myItem);
					myItem = null;
				}
			}else if (myItem != null) {
				patterns.add(myItem);
				myItem = null;
			}else if (otherItem != null) {
				patterns.add(otherItem);
				otherItem = null;
			}else {
				break;
			}
		}
		return new TopKStringPatterns(patterns);
	}
	
	@Override
	public void write(DataOutput out) throws IOException
	{
		// TODO Auto-generated method stub
		out.writeInt(frequentPatterns.size());
		for (Pair<List<String>, Long> pattern : frequentPatterns)
		{
			out.writeInt(pattern.getFirst().size());
			out.writeLong(pattern.getSecond());
			for (String item : pattern.getFirst())
			{
				out.writeUTF(item);
			}
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException
	{
		// TODO Auto-generated method stub
		frequentPatterns.clear();
		int length = in.readInt();
	    for (int i = 0; i < length; i++) {
	      List<String> items = new ArrayList<>();
	      int itemsetLength = in.readInt();
	      long support = in.readLong();
	      for (int j = 0; j < itemsetLength; j++) {
	        items.add(in.readUTF());
	      }
	      frequentPatterns.add(new Pair<List<String>,Long>(items, support));
	    }
	}

	@Override
	public String toString()
	{
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
	    String sep = "";
	    for (Pair<List<String>,Long> pattern : frequentPatterns) {
	      sb.append(sep);
	      sb.append(pattern.toString());
	      sep = ", ";
	      
	    }
	    return sb.toString();
	    
	}
}
