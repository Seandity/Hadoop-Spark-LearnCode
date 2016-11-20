package com.leishen.project.hadoop.datajoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by leishen on 2016/11/19 0019.
 */
public class JoinMapper extends Mapper<LongWritable, Text, Text, InfoBean> {




    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        FileSplit split = (FileSplit) context.getInputSplit();
        String fileName = split.getPath().getName();
        InfoBean bean = new InfoBean();
        String[] fields = line.trim().split(",");
        String id = fields[0];
        if ("fileName1".equals(fileName)) {

            String name = fields[1];
            int age = Integer.parseInt(fields[2]);
            String sex = fields[3];
            bean.set(id, name, age, sex, "", "1");
        } else {
            String department = fields[1];
            bean.set(id, "", 0, "", department, "12");
        }
        context.write(new Text(id), bean);
    }
}
