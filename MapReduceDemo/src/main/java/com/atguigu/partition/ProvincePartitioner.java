package com.atguigu.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author CZY
 * @date 2021/10/19 10:49
 * @description ProvincePartitioner
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean>{

    /**
     *
     * @param text the key to be partioned.
     * @param flowBean  the entry value.
     * @param numPartitions the total number of partitions.
     * @return
     */
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        //获取手机号前三位prePhone
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);

        //定义一个分区号变量partition，根据prePhone设置分区号
        int partition;
        if ("136".equals(prePhone)){
            partition = 0;
        }else if ("137".equals(prePhone)){
            partition = 1;
        }else if ("138".equals(prePhone)){
            partition = 2;
        }else if ("139".equals(prePhone)){
            partition = 3;
        }else{
            partition = 4;
        }
        return partition;
    }
}
