package com;


/**
 * @Descripion TODO
 * @Author Solarzhou
 * @Date 2020/5/21 10:41
 **/
public class Test01 {
    public static void main(String[] args) {
        System.out.printf("Hello world!");
        String str1 = "";
        String str2 = null;
        String str3 = null;
        System.out.println(str1 == str2);
        System.out.println(str1.equals(str2));
        System.out.println(str2 == str3);
    }
}