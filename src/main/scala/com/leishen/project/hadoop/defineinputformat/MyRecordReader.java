package com.leishen.project.hadoop.defineinputformat;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Created by leishen on 2016/11/20 0020.
 */
public class MyRecordReader extends RecordReader<NullWritable,ByteWritable> {
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

    }


    /**
     系统首先会先调用nextKeyValue来产生key 和 value的值，但是他没有返回值，
     只是返回一个boolean类型来告诉是否还有下一个值，
     通过getCurrentKey和getCurrentValue可以知道读取的key和value的值
     */
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }

    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    public ByteWritable getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    public void close() throws IOException {

    }
}
