官网：

用百度不太好找。。。废。。

[为什么开发ZenData - ZenData帮助文件 - ZenData测试数据生成器](https://www.zendata.cn/book/zendata/why-zendata-115.html)



# 下载安装

#### 一、Windows下载安装ZenData 

1. 从 https://zd.im/download.html下载ZenData最新版文件；
2. 解压后进入相应目录，如c:\zd；
3. 执行zd.exe -h获取使用帮助；
4. 执行 zd.exe -e获取命令示例；
5. 首次执行时，请根据提示选择工具的语言；
6. 根据提示执行setx Path命令，将zd.exe加入$PATH环境变量，以便于在任意目录中执行。

#### 二、Linux下面下载安装ZenData

1. 从 https://zd.im/download.html下载ZenData最新版文件；
2. 解压后进入相应目录，比如~/zd
3. 执行./zd -h获取使用帮助；
4. 执行 ./zd -e获取命令示例；
5. 首次执行时，请根据提示选择工具的语言；
6. ZenData将自动将zd命令加入用户环境变量$PATH， 以支持在任意目录中执行





# 问题：

## 1、生成decimal（10，2）随机数时

使用

```json
 - field: amount
    range: 0-10000000000:0.01
    rand: true
    postfix: ","
    format: "%.2f"
```

存在与生成数量有关系，比如生成10条，随机数在0.01，0.02，0.03，0.04，0.05，0.06，0.07，0.08，0.09之间

## 2、如何省市相互关联



## 3、生成文章后，如何转换为field