package com.atguigu.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * 计算一个给的基本数据类型的长度
 * @author CZY
 * @date 2021/10/29 10:23
 * @description MyUDF
 */
public class MyUDF extends GenericUDF {
    /**
     * 判断传进来的参数类型及长度
     * @param arguments
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        if (arguments.length !=1){
            throw new UDFArgumentLengthException("please give me  only one arg");
        }
        //PRIMITIVE基本数据类型
        if (!arguments[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(1,"i need primitive type arg");
        }
        //约定返回数据的类型
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    /**
     * 解决具体逻辑的
     * @param arguments
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        Object o = arguments[0].get();
        if (o == null){
            return 0;
        }
        return o.toString().length();
    }

    //用于获取解释的字符串
    @Override
    public String getDisplayString(String[] children) {
        return "";
    }
}
