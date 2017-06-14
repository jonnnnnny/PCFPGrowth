package com.jonny.pcfpgrowth.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.util.GenericsUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
*@author created by jonny
*@date 2017年5月10日--下午3:39:43
*
**/
public class Parameters {
	  
	  private static final Logger log = LoggerFactory.getLogger(Parameters.class);
	  
	  private Map<String,String> params = new HashMap<>();

	  public Parameters() {

	  }

	  public Parameters(String serializedString) throws IOException {
	    this(parseParams(serializedString));
	  }

	  protected Parameters(Map<String,String> params) {
	    this.params = params;
	  }

	  public String get(String key) {
	    return params.get(key);
	  }
	  
	  public String get(String key, String defaultValue) {
	    String ret = params.get(key);
	    return ret == null ? defaultValue : ret;
	  }
	  
	  public void set(String key, String value) {
	    params.put(key, value);
	  }
	  
	 
	  public int getInt(String key, int defaultValue) {
	    String ret = params.get(key);
	    return ret == null ? defaultValue : Integer.parseInt(ret);
	  }

	  @Override
	  public String toString() {
	    Configuration conf = new Configuration();
	    conf.set("io.serializations",
	             "org.apache.hadoop.io.serializer.JavaSerialization,"
	             + "org.apache.hadoop.io.serializer.WritableSerialization");
	  //DefaultStringifier 用来从configuration对象中get或set key,value键值对的时候使用。
	    DefaultStringifier<Map<String,String>> mapStringifier = new DefaultStringifier<Map<String,String>>(conf,
	        GenericsUtil.getClass(params));
	    try {
	      return mapStringifier.toString(params);
	    } catch (IOException e) {
	      log.info("Encountered IOException while deserializing returning empty string", e);
	      return "";
	    }
	    
	  }
	  
	  public String print() {
	    return params.toString();
	  }

	  public static Map<String,String> parseParams(String serializedString) throws IOException {
	    Configuration conf = new Configuration();
	    conf.set("io.serializations",
	             "org.apache.hadoop.io.serializer.JavaSerialization,"
	             + "org.apache.hadoop.io.serializer.WritableSerialization");
	    Map<String,String> params = Maps.newHashMap();
	    DefaultStringifier<Map<String,String>> mapStringifier = new 
	                  DefaultStringifier<Map<String,String>>(conf,
	        GenericsUtil.getClass(params));
	    return mapStringifier.fromString(serializedString);
	  }

	}

