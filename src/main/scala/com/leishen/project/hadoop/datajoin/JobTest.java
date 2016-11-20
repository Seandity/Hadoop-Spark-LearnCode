package com.leishen.project.hadoop.datajoin;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

/**
 * Created by leishen on 2016/11/19 0019.
 */
public class JobTest {

    public static void main(String[] args){
        Configuration config = new Configuration();
        try {
            Job job = Job.getInstance(config);

            job.setJarByClass(JobTest.class);

            job.setMapperClass(JoinMapper.class);
            job.setReducerClass(JoinReducer.class);

            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(InfoBean.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);


        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
