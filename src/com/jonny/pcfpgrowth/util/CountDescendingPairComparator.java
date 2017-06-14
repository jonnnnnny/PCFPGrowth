package com.jonny.pcfpgrowth.util;

import java.io.Serializable;
import java.util.Comparator;


/**
*@author created by jonny
*@date 2017年5月16日--下午3:41:02
*
**/
public final class CountDescendingPairComparator<A extends Comparable<? super A>, B extends Comparable<? super B>>
implements Comparator<Pair<A, B>>, Serializable {

public int compare(Pair<A, B> a, Pair<A, B> b) {
int ret = b.getSecond().compareTo(a.getSecond());
if (ret != 0) {
    return ret;
}
return a.getFirst().compareTo(b.getFirst());
}
}
