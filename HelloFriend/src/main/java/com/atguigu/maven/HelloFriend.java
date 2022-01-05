package com.atguigu.maven;

import org.junit.Test;

/**
 * @author C Y
 * @date 2021/10/8 15:02
 * @description HelloFriend
 */
public class HelloFriend {
    public String sayHelloToFriend(String name){
        Hello hello = new Hello();
        String str = hello.sayHello(name) + " I am " + this.getMyName();
        return str;
    }

    public String getMyName() {
        return "Idea";
    }

    //HelloFriend的pom.xml文件依赖范围使用默认compile
    @Test
    public void UtilTest(){

    }
}
