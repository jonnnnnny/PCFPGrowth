package com.jonny.pcfpgrowth.ItemSet;
import java.util.List;

/**
*@author created by jonny
*@date 2017年6月7日--下午2:10:56
*
**/
public class ItemSet extends AbstractOrderedItemset
{

	public int[] itemset;
	public long support = 0;
	
	public int[] getItems(){
		return itemset;
	}
	
	public ItemSet()
	{
		itemset = new int[]{};
	}
	
	public ItemSet(int item)
	{
		itemset = new int[]{item};
	}
	
	public ItemSet(int[] items)
	{
		this.itemset = items;
	}
	
	public ItemSet(List<Integer> itemset, long support)
	{
		this.itemset = new int[itemset.size()];
		int i = 0;
		for (Integer item : itemset)
		{
			this.itemset[i++] = item.intValue();
		}
		this.support = support;
	}
	
	public Long getAbsoluteSupport()
	{
		return support;
	}
	
	public int size()
	{
		return itemset.length;
	}
	
	public Integer get(int position)
	{
		return itemset[position];
	}
	
	public void setAbsoluteSupport(Long support)
	{
		this.support = support;
	}
	
	public void increaseTransactionCount()
	{
		this.support++;
	}
	
	public ItemSet cloneItemSetMinusOneItem(Integer itemToRemove)
	{
		int[] newItemSet = new int[itemset.length -1];
		int i = 0;
		for(int j = 0; j < itemset.length; j++)
		{
			if (itemset[j] != itemToRemove)
			{
				newItemSet[i++] = itemset[j];
			}
		}
		return new ItemSet(newItemSet);
	}
	
	
	public ItemSet cloneItemSetMinuAnItemSet(ItemSet itemSetToNotKeep)
	{
		// create a new itemset
				int[] newItemset = new int[itemset.length - itemSetToNotKeep.size()];
				int i=0;
				// for each item of this itemset
				for(int j =0; j < itemset.length; j++){
					// copy the item except if it is not an item that should be excluded
					if(itemSetToNotKeep.contains(itemset[j]) == false){
						newItemset[i++] = itemset[j];
					}
				}
				return new ItemSet(newItemset); // return the copy
	}
	
	
	
	
}
