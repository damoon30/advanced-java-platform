package com.advanced.spark.socket;

import com.alibaba.fastjson.JSON;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

public class Executor {


    /**
     * 最基础的网络编程模型
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException {


        ServerSocket serverSocket = new ServerSocket(9999);
        System.out.println("服务器启动，等待数据连接");
        Socket client = serverSocket.accept();
        InputStream inputStream = client.getInputStream();
        ObjectInputStream objectInputStream = new ObjectInputStream(inputStream);
        Object o = objectInputStream.readObject();

        if (o instanceof Task) {
            List<Integer> compute = ((Task) o).compute();
            System.out.println("计算节点的值：" + JSON.toJSONString(compute));
        }
        System.out.println("接受到客户端的数据：" + JSON.toJSONString(o));
        inputStream.close();
        client.close();
        serverSocket.close();
    }
}
