package com.zhileiedu.mr2;

import com.zhileiedu.mr1.WriteFruitMRReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/3/6 22:04
 */
public class Driver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration configuration = HBaseConfiguration.create();
		configuration.set("hbase.zookeeper.quorum", "hadoop01,hadoop02,hadoop03,hadoop04");

		Job job = Job.getInstance(configuration);
		job.setJarByClass(Driver.class);
		job.setMapperClass(ReadFruitFromHDFSMapper.class);
		job.setReducerClass(WriteFruitMRReducer.class);

		job.setMapOutputKeyClass(ImmutableBytesWritable.class);
		job.setMapOutputValueClass(Put.class);

		FileInputFormat.setInputPaths(job, new Path("/input_fruit"));

		TableMapReduceUtil.initTableReducerJob("fruit_mr", WriteFruitMRReducer.class, job);

		boolean b = job.waitForCompletion(true);

		System.exit(b ? 0 : 1);

	}
}
