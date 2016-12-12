package com.leishen.project.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Created by leishen on 2016/12/12 0012.
 */
public class FSAPIPractice {

    public static void main(String[] args) throws IOException {

        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(configuration);

        LocalFileSystem localFileSystem = fs.getLocal(configuration);

        FSDataInputStream inputStream = localFileSystem.open(new Path("L:\\word.txt"));

        FileOutputStream stream = new FileOutputStream(new File("L:\\word1.txt"));

        IOUtils.copyBytes(inputStream,stream,configuration);
    }
}
