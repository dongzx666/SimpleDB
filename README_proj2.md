# cs186-proj2

## 流程
1. 补全所有函数并完成单测
2. 运行`mvn package assembly:single`打成jar包
3. 和proj1一样转出`some_data_file1.dat`and`some_data_file2.dat`
4. 复制jointest运行, 然后进行进一步的查询解析器
5. 创建`data.txt`并复制内容, 命令行`java -jar target/simpledb-jar-with-dependencies.jar convert data.txt 2 "int,int"`
6. 创建`catalog.txt`并复制内容, `java -jar target/simpledb-jar-with-dependencies.jar parser catalog.txt`
7. 在解析器内输入`select d.f1, d.f2 from data d;`得出结果

## 感悟

### Predicate
1. 理解概念，以及这个类的目的

[SQL Server关于predicate、density、selectivity、cardinality名词浅析](https://blog.csdn.net/weixin_30576827/article/details/95234203)

### Join
1. Join传统有三种算法, NLJ(Nested Loop Join), Hash Join, Sort-Merge Join

[常见的join算法](https://blog.csdn.net/u010670689/article/details/79964185)

### Aggregator
1. 开发时要理解聚合函数和group-by，本项目返回的Tuple很简单，只含两个值<分组项，聚合结果>
2. 如果没有group-by，相当于只对这个字段整列做聚合，返回<聚合结果>

[mysql中group by的时候字段不加聚合函数和distinct的情况](https://blog.csdn.net/wxwzy738/article/details/20636563)

[MYSQL中GROUP BY不包含所有的非聚合字段](https://blog.csdn.net/liufei198613/article/details/82979034)

[SQL中只要用到聚合函数就一定要用到group by 吗？](https://zhidao.baidu.com/question/103627012.html)

[关于group by的用法 原理](https://blog.csdn.net/u014717572/article/details/80687042)

## bufferpool

[LRU缓存算法（Java实现）](https://www.jianshu.com/p/95b6f10ed1f3)

## 其他

1. Java的List的初始化赋值，不是往括号写(括号里填的是装载因子)，二是双大括号

[Java中初始化List的5种方法示例](https://www.jb51.net/article/150596.htm)

2. maven打jar包

[Maven生成可以直接运行的jar包的多种方式](https://blog.csdn.net/xiao__gui/article/details/47341385)
[使用maven编译Java项目](https://waylau.com/build-java-project-with-maven/)
