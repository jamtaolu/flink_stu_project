package com.taolu.flink.util;

import java.io.*;
import java.util.ArrayList;

public class ReadUtil {

    public static ArrayList<String> readFileStr(File file){

        FileReader fr = null;
        // 使用ArrayList来存储每行读取到的字符串
        ArrayList<String> arrayList = new ArrayList<>();
        try {
            fr = new FileReader(file);
            BufferedReader bf = new BufferedReader(fr);
            String str;
            // 按行读取字符串
            while ((str = bf.readLine()) != null) {
                arrayList.add(str);
            }
            bf.close();
            fr.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return arrayList;

    }
}
