package com.zhileiedu.mr1.tool;

import com.zhileiedu.mr1.ReadFruitMapper;
import com.zhileiedu.mr1.WriteFruitMRReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/3/6 21:43
 */
public class Driver {// driver另一种的写法

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03,hadoop04");

		Job job = Job.getInstance(configuration);
		job.setJarByClass(Driver.class);

		// 处理mapper
		Scan scan = new Scan();
		TableMapReduceUtil.initTableMapperJob("fruit",
				scan, ReadFruitMapper.class, ImmutableBytesWritable.class, Put.class, job);

		// 处理Reduce
		job.setNumReduceTasks(100);
		TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitMRReducer.class, job, HRegionLocation.class);

		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}
}
