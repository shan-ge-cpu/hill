![image-20220321141017950](flink熊老师.assets/image-20220321141017950-16478430195191.png)



![image-20220321142138797](flink熊老师.assets/image-20220321142138797-16478436998562.png)

![image-20220321142456183](flink熊老师.assets/image-20220321142456183-16478438972253.png)



![image-20220321143154237](flink熊老师.assets/image-20220321143154237-16478443163904.png)



![image-20220321143348961](flink熊老师.assets/image-20220321143348961-16478444306465.png)



![image-20220321143656062](flink熊老师.assets/image-20220321143656062-16478446171607.png)





![image-20220321143827221](flink熊老师.assets/image-20220321143827221-16478447084708.png)



start_cluster

> 做了两步工作
>
> 一、启动jobManager
>
> 二、启动taskManager





看源码的方向：

入口  -- 》 主要模块

![image-20220321144532606](flink熊老师.assets/image-20220321144532606-16478451335729.png)





![image-20220321145525078](flink熊老师.assets/image-20220321145525078-164784572624010.png)





CliFronted.java

main

> 提交代码的入口



平台代码隔离：

> 利用classLoader进行隔离



classloader

> 自己看