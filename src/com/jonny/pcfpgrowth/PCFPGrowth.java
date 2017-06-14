package com.jonny.pcfpgrowth;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jonny.pcfpgrowth.util.Parameters;

/**
 * @author created by jonny
 * @date 2017年5月10日--下午3:25:22
 *
 **/
public class PCFPGrowth
{

	private static final Logger log = LoggerFactory.getLogger(PCFPGrowth.class);
	// 输入的参数组最小长度
	private static final int ARG_LEN = 6;

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException
	{
		if (args.length < ARG_LEN)
		{
			System.err.println(
					"Usage: PCFPGrowth <input_file> <output_directory> <discretize (true|false)> <minSupportThreshod (0.0|1.0)> <Least_support (0.0|1.0)> [<min_confidence (0.0|1.0)>]");
			System.exit(-1);
		}
		// 参数
		//输入目录
		String input = args[1];
		//输出目录
		String output = args[2];
		// 是否离散化
		Integer enableDiscretization = (args[3].equals("true")) ? 1 : 0;
		
		Integer enableRules;
		//基于概率 来设置 多重最小支持度，
		//即 统计出所有item的支持度后， 根据用户给出的支持度阈值设置每个item的最小支持度
		//同时 预设一个 最低的支持度LS
		// MIS(i) = Support * minSupportThreshold  > LS  or LS
		Double minSupportThreshod = new Double(args[4]);
		Double leastSupport = new Double(args[5]);
		Double minConfidence = null;
		System.err.println(ARG_LEN);
		//设置置信度
		if (args.length == (ARG_LEN + 1))
		{
			enableRules = new Integer(1);
			minConfidence = new Double(args[6]);
		}
		else
		{
			enableRules = new Integer(0);
		}

		// 数据集的事务中每个item之间的分割符
		String splitPattern = "[\\ ]";
		
		Parameters params = new Parameters();
		params.set("minSupportThreshod", minSupportThreshod.toString());
		params.set("leastSupport",leastSupport.toString());
		if (enableRules.compareTo(new Integer(1)) == 0) {
            params.set("minConfidence", minConfidence.toString());
        }
		
		params.set("splitPattern", splitPattern);
		params.set("input", input);
		params.set("output", output);
		params.set("enableDiscretization", enableDiscretization.toString());
        params.set("enableRules", enableRules.toString());
        
        log.info("========================| PCFPGrowth |=======================");
        log.info("=== A cloud-based Service for Association RUle Mining ===");
        log.info("============== Developed by Jonny ==============");
        log.info("Input file: " + input);
        log.info("Output directory: " + output);
        log.info("MinSupportThreshod: " + minSupportThreshod.toString());
        log.info("LeastSupport: " + leastSupport.toString());
        if (enableRules.compareTo(new Integer(1)) == 0) {
            log.info("MinConf: " + minConfidence.toString());
        }
		
        Parallel.run(params);
	}
}
