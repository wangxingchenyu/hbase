package com.zhileiedu.mr2;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/3/6 21:51
 */
public class ReadFruitFromHDFSMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String lineValue = value.toString();
		String[] values = lineValue.split("\t");

		//根据数据中值的含义取值
		String rowKey = values[0];
		String nameValue = values[1];
		String colorValue = values[2];

		// 初始化RowKey
		ImmutableBytesWritable rowKeyWritable = new ImmutableBytesWritable(Bytes.toBytes(rowKey));

		Put put = new Put(Bytes.toBytes(rowKey));

		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(nameValue));
		put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("color"), Bytes.toBytes(colorValue));

		context.write(rowKeyWritable, put);
	}
}
