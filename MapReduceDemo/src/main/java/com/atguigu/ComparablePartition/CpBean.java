package com.atguigu.ComparablePartition;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author CZY
 * @date 2021/10/19 19:19
 * @description FlowBean
 */
public class CpBean implements WritableComparable<CpBean>{
    private Integer upFlow;
    private Integer downFlow;
    private Integer sumFlow;

    public CpBean() {
    }

    public Integer getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(Integer upFlow) {
        this.upFlow = upFlow;
    }

    public Integer getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(Integer downFlow) {
        this.downFlow = downFlow;
    }

    public Integer getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(Integer sumFlow) {
        this.sumFlow = sumFlow;
    }

    public void setSumFlow(){

        this.sumFlow=this.upFlow+this.downFlow;
    }

    @Override
    public String toString() {
        return upFlow+"\t"+downFlow+"\t"+sumFlow;
    }

    @Override
    public int compareTo(CpBean o) {
        if (this.sumFlow < o.sumFlow){
            return 1;
        }else if (this.sumFlow > o.sumFlow){
            return -1;
        }else {
            if (this.upFlow < o.upFlow){
                return -1;
            }else if (this.upFlow > o.upFlow){
                return 1;
            }else {
                return 0;
            }
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(upFlow);
        out.writeInt(downFlow);
        out.writeInt(sumFlow);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readInt();
        downFlow = in.readInt();
        sumFlow = in.readInt();
    }
}
