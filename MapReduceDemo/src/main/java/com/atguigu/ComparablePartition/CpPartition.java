package com.atguigu.ComparablePartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author CZY
 * @date 2021/10/19 19:51
 * @description CpPartition
 */
public class CpPartition extends Partitioner<CpBean, Text> {
    @Override
    public int getPartition(CpBean cpBean, Text text, int numPartitions) {
        //获取手机号前三位
        String phone = text.toString();
        String prePhone = phone.substring(0, 3);

        //定义一个分区号变量partition,根据prePhone设置分区号
        int partition=0;
        switch (prePhone) {
            case "136":
                partition = 0;
                break;
            case "137":
                partition = 1;
                break;
            case "138":
                partition = 2;
                break;
            case "139":
                partition = 3;
                break;
            default:
                partition = 4;
        }
        return partition;

    }
}
