package com.zhileiedu.mr1;

import com.zhileiedu.mr1.tool.Fruit2FruitMRRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author: wzl
 * @Date: 2020/3/6 16:35
 */
public class MDriver { // 利用mapreduce 将hbase里面一张表复制到别一张表里面
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03,hadoop04");
		int status = ToolRunner.run(conf, new Fruit2FruitMRRunner(), args);
		System.exit(status);

	}
}
