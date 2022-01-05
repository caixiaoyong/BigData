package com.atguigu.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author CZY
 * @date 2021/10/20 10:52
 * @description TableReducer
 * 接受Mapper传入的Text,TableBead，再由Reducer以TableBean输出，因为其包含着打印的所有内容
 */
public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable> {
    //3.由于集合和goods对象在方法reduce中new的太多了，所以拿到外面来
    ArrayList<TableBean> orderList = new ArrayList<>();
    TableBean goods = new TableBean();

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        /**
         * 4.考虑到对象是在类中声明的，所以在reduce方法使用时将反复的循环迭代，
         * 所以考虑每次调用前先清空
         */
            orderList.clear();
        for (TableBean value : values) {
            if ("order".equals(value.getFlag())){
                /*
                TableBean tb = new TableBean();
                tb.setId(value.getId());
                tb.setPid(value.getPid());
                tb.setAmount(value.getAmount());
                tb.setPname(value.getPname());
                tb.setFlag(value.getFlag());*/
                //创建一个临时TableBean对象接收value
                TableBean tb = new TableBean();
                try {
                    /**
                     * 5.考虑到当属性过多时，上面方法将很累赘，
                     * 所以BeanUtils包下的方法copyProperties
                     * 将原始数据值赋给目标数据
                     */
                    BeanUtils.copyProperties(tb,value);

                } catch (Exception e) {
                    e.printStackTrace();
                }
                //1.拿集合来装订单数据
                orderList.add(tb);
            }else {
                //2.拿对象来装唯一商品数据
                /*goods.setId(value.getId());
                goods.setPid(value.getPid());
                goods.setAmount(value.getAmount());
                goods.setPname(value.getPname());
                goods.setFlag(value.getFlag());*/
                try {
                    BeanUtils.copyProperties(goods,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        //遍历集合orderList，将商品表的Pname替换订单表的Pid
        for (TableBean tableBean : orderList) {
            tableBean.setPname(goods.getPname());
            context.write(tableBean,NullWritable.get());
        }
    }
}
