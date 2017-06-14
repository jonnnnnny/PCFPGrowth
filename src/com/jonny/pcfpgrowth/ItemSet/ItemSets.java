package com.jonny.pcfpgrowth.ItemSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
*@author created by jonny
*@date 2017年6月7日--下午2:09:10
*
**/
public class ItemSets
{

	private final List<List<ItemSet>> levels = new ArrayList<>();
	
	private int itemsetsCount = 0;
	
	private String name;
	
	public ItemSets(String name)
	{
		this.name = name;
		levels.add(new ArrayList<ItemSet>());
	}
	
	public void printItemsets(int nbObject) {
		System.out.println(" ------- " + name + " -------");
		int patternCount = 0;
		int levelCount = 0;
		// for each level (a level is a set of itemsets having the same number of items)
		for (List<ItemSet> level : levels) {
			// print how many items are contained in this level
			System.out.println("  L" + levelCount + " ");
			// for each itemset
			for (ItemSet itemset : level) {
				Arrays.sort(itemset.getItems());
				// print the itemset
				System.out.print("  pattern " + patternCount + ":  ");
				itemset.print();
				// print the support of this itemset
				System.out.print("support :  "
						+ itemset.getRelativeSupportAsString(nbObject));
				patternCount++;
				System.out.println("");
			}
			levelCount++;
		}
		System.out.println(" --------------------------------");
	}
	
	public void addItemSet(ItemSet itemSet, int k)
	{
		while(levels.size() <= k)
		{
			levels.add(new ArrayList<ItemSet>());
		}
		levels.get(k).add(itemSet);
		itemsetsCount++;
	}
	
	public List<List<ItemSet>> getLevels()
	{
		return levels;
	}
	
	public int getItemsetsCount(){
		return itemsetsCount;
	}
	
	public void setName(String newName)
	{
		name = newName;
	}
	
	public void decreaseItemsetCount() {
		itemsetsCount--;
	}
}
