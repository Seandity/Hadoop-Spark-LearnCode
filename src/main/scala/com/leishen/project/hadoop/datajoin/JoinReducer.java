package com.leishen.project.hadoop.datajoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by leishen on 2016/11/19 0019.
 */
public class JoinReducer extends Reducer<Text, InfoBean, Text, Text> {

    private int id;
    private String name;
    private int age;
    private String sex;
    private String department;
    private String flag;

    @Override
    protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {

        List<InfoBean> list1 = new ArrayList<InfoBean>();
        List<InfoBean> list2 = new ArrayList<InfoBean>();

        for (InfoBean bean : values) {
            if ("1".equals(bean.getFlag())) {
                list1.add(bean);
            } else {
                list2.add(bean);
            }
        }
        for (InfoBean bean : list1) {
            for (InfoBean bean1 : list2) {
                String id = bean.getId();
                String name = bean.getName();
                int age = bean.getAge();
                String sex = bean.getSex();
                String department = bean1.getDepartment();
                context.write(new Text(id),new Text(id+","+name+","+age+","+sex+","+department+","));
            }
        }
    }
}
