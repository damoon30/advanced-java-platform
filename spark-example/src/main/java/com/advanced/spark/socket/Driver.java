package com.advanced.spark.socket;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.Socket;

public class Driver {

    /**
     * 最基础的网络编程模型
     */
    public static void main(String[] args) throws IOException {

        //连接服务器
        Socket client = new Socket("localhost", 9999);


        OutputStream outputStream = client.getOutputStream();
        ObjectOutputStream objectOutputStream = new ObjectOutputStream(outputStream);
        objectOutputStream.writeObject(new Task());
        objectOutputStream.flush();
        objectOutputStream.close();
        System.out.println("已发送2");
        outputStream.close();
        client.close();
    }
}
