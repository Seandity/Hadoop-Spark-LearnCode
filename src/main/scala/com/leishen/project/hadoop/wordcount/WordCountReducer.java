package com.leishen.project.hadoop.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by shenlei on 2016/11/5.
 */
public class WordCountReducer extends Reducer<Text,LongWritable,Text,LongWritable>{

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;

        for(LongWritable number : values){
            count += number.get();
        }

        context.write(key,new LongWritable(count));
    }
}
