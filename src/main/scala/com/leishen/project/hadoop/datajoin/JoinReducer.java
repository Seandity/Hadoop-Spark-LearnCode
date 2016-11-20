package com.leishen.project.hadoop.datajoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by leishen on 2016/11/19 0019.
 */
public class JoinReducer extends Reducer<Text, InfoBean, Text, Text> {



    @Override
    protected void reduce(Text key, Iterable<InfoBean> values, Context context) throws IOException, InterruptedException {


        List<InfoBean> list2 = new ArrayList<InfoBean>();

        InfoBean bean1 = new InfoBean() ;

        for(InfoBean bean : values){
           // String flag = bean.getFlag();
            if ("1".equals(bean.getFlag())) {
                try {
                    BeanUtils.copyProperties(bean1,bean);
                } catch(Exception e) {
                    e.printStackTrace();
                }
            } else if("2".equals(bean.getFlag())) {
                InfoBean bb = new InfoBean();
                try {
                    BeanUtils.copyProperties(bb,bean);
                    list2.add(bb);
                } catch(Exception e) {
                    e.printStackTrace();
                }
            }
        }

            for (InfoBean bean2 : list2) {
                String id = bean1.getId();
                String name = bean1.getName();
                int age = bean1.getAge();
                String sex = bean1.getSex();
                String department = bean2.getDepartment();
                Text text = new Text(id+","+name+","+age+","+sex+","+department);
                context.write(new Text(id),text);
            }

    }
}
