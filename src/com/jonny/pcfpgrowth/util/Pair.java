package com.jonny.pcfpgrowth.util;

import java.io.Serializable;


/**
*@author created by jonny
*@date 2017年5月12日--上午9:22:44
*
**/
public class Pair <A, B> implements Comparable<Pair<A, B>>, Serializable
{

	private final A first;
	private final B second;
	
	public Pair(A first, B second)
	{
		this.first = first;
		this.second = second;
		// TODO Auto-generated constructor stub
	}
	
	public A getFirst()
	{
		return first;
	}
	
	public B getSecond()
	{
		return second;
	}
	
	public Pair<B, A> swap()
	{
		return new Pair<B, A>(second, first);
	}
	
	@Override
	public boolean equals(Object obj)
	{

		if (!(obj instanceof Pair<?, ?>))
		{
			return false;
		}
		Pair<?, ?> otherPair = (Pair<?, ?>) obj;
		return isEqualOrNulls(first, otherPair.getFirst())
				&& isEqualOrNulls(second, otherPair.getSecond());
		// TODO Auto-generated method stub
	}

	private boolean isEqualOrNulls(Object obj1, Object obj2)
	{
		// TODO Auto-generated method stub
		return obj1 == null ? obj2 == null : obj1.equals(obj2);
	}
	
	
	@Override
	public int hashCode()
	{
		int firstHash = hashCodeNull(first);
		return (firstHash >>> 16 | firstHash << 16) ^ hashCodeNull(second);
	}

	private int hashCodeNull(Object obj)
	{
		// TODO Auto-generated method stub
		
		return obj == null ? 0 : obj.hashCode();
	}
	
	
	@Override
	public String toString()
	{
		// TODO Auto-generated method stub
		return '(' + String.valueOf(first) + ',' + second + ')';
	}
	
	@Override
	public int compareTo(Pair<A, B> o)
	{
		// TODO Auto-generated method stub
		Comparable<A> thisFirst = (Comparable<A>) first;
		A thatFirst = o.getFirst();
		int compare = thisFirst.compareTo(thatFirst);
		if (compare != 0)
		{
			return compare;
		}
		
		Comparable<B> thisSecond = (Comparable<B>) second;
		B thatSecond = o.getSecond();
		
		return thisSecond.compareTo(thatSecond);
	}

}
