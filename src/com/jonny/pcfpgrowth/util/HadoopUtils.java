package com.jonny.pcfpgrowth.util;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
*@author created by jonny
*@date 2017年5月10日--下午4:24:14
*
**/
public class HadoopUtils
{

private static final Logger log = LoggerFactory.getLogger(HadoopUtils.class);
	
	private HadoopUtils(){}
	
	public static void delete(Configuration conf, Iterable<Path> paths) throws IOException
	{
		if (conf == null)
		{
			conf = new Configuration();
		}
		
		for(Path path : paths)
		{
			FileSystem fs = path.getFileSystem(conf);
			if (fs.exists(path))
			{
				log.info("Deleting{}",path);
				fs.delete(path,true);
			}
		}
	}
	public static void delete(Configuration conf, Path... paths) throws IOException
	{
		delete(conf, Arrays.asList(paths));
	}
	
	public static FileStatus[] getFileStatus(Path path, PathType pathType, 
			PathFilter filter, Comparator<FileStatus> ordering, Configuration conf) throws IOException
	{
		FileStatus[] statuses;
		FileSystem fs = path.getFileSystem(conf);
		if (filter == null)
		{
			statuses = pathType == PathType.GLOB ? fs.globStatus(path) : listStatus(fs, path);
		}else {
			statuses = pathType == PathType.GLOB ? fs.globStatus(path, filter) : listStatus(fs, path, filter);
		}
		
		if (ordering != null)
		{
			Arrays.sort(statuses, ordering);
		}
		
		return statuses;
	}

	private static FileStatus[] listStatus(FileSystem fs, Path path, PathFilter filter) throws IOException
	{
		try
		{
			return fs.listStatus(path, filter);
		}
		catch (FileNotFoundException e)
		{
			return new FileStatus[0];
		}
		
		
	}

	private static FileStatus[] listStatus(FileSystem fs, Path path) throws IOException
	{
		try
		{
			return fs.listStatus(path);
		}
		catch (FileNotFoundException e)
		{
			// TODO Auto-generated catch block
			return new FileStatus[0];
		}
		
	}
}
