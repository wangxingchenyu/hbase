package com.zhileiedu.hbaseClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

/**
 * @Author: wzl
 * @Date: 2020/3/5 20:59
 */
public class HbaseClient2 {

	private Configuration configuration;
	private Connection connection;
	private TableName atguigu;

	@Before
	public void connect() throws IOException {
		configuration = HBaseConfiguration.create();
		connection = ConnectionFactory.createConnection(configuration);
		atguigu = TableName.valueOf("atguigu:student01");
	}

	@Test
	public void selectLines() throws IOException { // 查询多行
		Table table = connection.getTable(atguigu);
		ResultScanner scanner = table.getScanner(new Scan());
		for (Result result : scanner) {
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

				System.out.println("行尾符号................................................");
			}
		}

	}

	@Test
	public void deleteLines() throws IOException { // 删除多行的数据
		Table table = connection.getTable(atguigu);
		ArrayList<Delete> deletes = new ArrayList<Delete>();

		ArrayList<String> rows = new ArrayList<String>();
		rows.add("1001");
		rows.add("1002");
		for (String row : rows) {
			Delete delete = new Delete(Bytes.toBytes(row));
			deletes.add(delete);
		}
		table.delete(deletes);
	}

	@Test
	public void deleteDataByRowKey() throws IOException {
		Table table = connection.getTable(atguigu);
		String rowKey = "1001";
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		delete.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
		//delete.addColumn() delete 可以添加删除的列，删除的family,。。。
		table.delete(delete);

	}

	@Test
	public void deleteTable() throws IOException {
		// 删除表
		Admin admin = connection.getAdmin();

		if (admin.tableExists(atguigu)) {
			admin.disableTable(atguigu);
			// 删除表
			admin.deleteTable(atguigu);
		}
	}

	@Test
	public void createregion() throws IOException { // 手动创建预分区
		Admin admin = connection.getAdmin();
		HTableDescriptor hTableDescriptor = new HTableDescriptor(atguigu);
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(Bytes.toBytes("info"));
		hTableDescriptor.addFamily(hColumnDescriptor);
		byte[][] bs = new byte[3][];
		bs[0] = Bytes.toBytes("10001");
		bs[1] = Bytes.toBytes("10002");
		bs[2] = Bytes.toBytes("10003");

		admin.createTable(hTableDescriptor, bs);
	}


}
