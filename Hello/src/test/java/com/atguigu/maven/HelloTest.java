package com.atguigu.maven;

import org.junit.Test;
import static junit.framework.Assert.*;
/**
 * @author C Y
 * @date 2021/10/8 10:59
 * @description HelloTest
 */
public class HelloTest {
    @Test
    public void HelloTest(){
        Hello hello = new Hello();
        String re = hello.sayHello("atguigu");
        //断言--判断结果和预想的结果是否相同
        assertEquals("Hello atguigu!",re);
    }
}
