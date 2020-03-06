package com.zhileiedu.hbaseClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @Author: wzl
 * @Date: 2020/3/5 20:59
 */
public class HbaseClient {

	public static void main(String[] args) throws IOException {
		Configuration configuration = HBaseConfiguration.create();
		Connection connection = ConnectionFactory.createConnection(configuration);
		// 获取操作对象
		Admin admin = connection.getAdmin();

		// 判断Hbase中，是否存在某张表
		TableName student = TableName.valueOf("atguigu:student");
		boolean flag = admin.tableExists(student);

		// 名字空间创建
		try {
			admin.getNamespaceDescriptor("atguigu");
		} catch (NamespaceNotFoundException e) { //如果上面的one namespace未找到，则抛出异常
			NamespaceDescriptor atguigu = NamespaceDescriptor.create("atguigu").build();
			admin.createNamespace(atguigu);
		}

		if (flag) {
			// 执行查询
			Table table = connection.getTable(student);
			String rowKey = "1001";
			Get get = new Get(Bytes.toBytes(rowKey));

			// 返回查询结果
			Result result = table.get(get);

			if (result.isEmpty()) {
				// 新增
				Put put = new Put(Bytes.toBytes(rowKey));
				String cf = "info";
				String cm = "name";
				String val = "张三";
				put.addColumn(Bytes.toBytes(cf), Bytes.toBytes(cm), Bytes.toBytes(val));
				table.put(put);
				System.out.println("增加数据.......");
			} else {
				// 查询
				for (Cell cell : result.rawCells()) {

					byte[] value = CellUtil.cloneValue(cell);
					byte[] family = CellUtil.cloneFamily(cell);
					byte[] row = CellUtil.cloneRow(cell);
					byte[] qualifier = CellUtil.cloneQualifier(cell);

					System.out.println("查询数据...............");
					System.out.println("value=" + Bytes.toString(value));
					System.out.println("family=" + Bytes.toString(family));
					System.out.println("row=" + Bytes.toString(row));
					System.out.println("qualifier=" + Bytes.toString(qualifier));
				}
			}


		} else {
			// 没有查到，则执行，创建表
			HTableDescriptor td = new HTableDescriptor(student);
			HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
			td.addFamily(hColumnDescriptor);
			admin.createTable(td);
			System.out.println("表创建成功....");
		}
	}
}
