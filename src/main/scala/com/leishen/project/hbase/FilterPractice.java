package com.leishen.project.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by leishen on 2016/12/21 0021.
 */
public class FilterPractice {

    public static void main(String[] rag) throws IOException {
        Configuration config = HBaseConfiguration.create();
        //设置zookeeper的服务地址，具体自己设置
        config.set("hbase.zookeeper.quorum", "192.168.10.149,192.168.10.44,192.168.10.49");

        config.set("hbase.zookeeper.property.clientPort", "2181");
        //得到连接
        Connection con = ConnectionFactory.createConnection(config);

        Admin admin = con.getAdmin();

        TableName name = TableName.valueOf("test");

        HTableDescriptor tableDesc = new HTableDescriptor(name);

        tableDesc.addFamily(new HColumnDescriptor("liezu"));
        //创建HTable表
        admin.createTable(tableDesc);
        //得到表
        Table table = con.getTable(TableName.valueOf("test"));

        List<Filter> filterList = new ArrayList<Filter>();

        Filter row = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator("[5-9]A"));

        filterList.add(row);

        Filter fam = new FamilyFilter(CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes("column1")));

        filterList.add(fam);

        Scan scan = new Scan();

        scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL, filterList));

        ResultScanner results = table.getScanner(scan);

        for (Result result : results) {
            System.out.println(result);
        }


    }
}
