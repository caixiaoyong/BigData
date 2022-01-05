package com.atguigu.maven;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author C Y
 * @date 2021/10/8 15:03
 * @description HelloFriendTest
 */
public class HelloFriendTest {
    @Test
    public void testHelloFriend(){
        HelloFriend helloFriend = new HelloFriend();
        String rs = helloFriend.sayHelloToFriend("Maven");
        //断言 判断结果和预想的结果是否相同
        assertEquals("Hello Maven! I am Idea",rs);
    }
}
