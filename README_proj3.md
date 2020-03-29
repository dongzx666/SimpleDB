# CS186-proj3

## 感悟

### Logical Plan

1. 概念(logical plan是什么，处于sql查询或解析的哪个阶段，或者处于哪个部分)

> 这一步要做的事情有点类似语义分析。在这个阶段里面，主要有两个工作：

> 对所有在语句里引用到的实体进行合法性检查。这里说的实体主要是表和表的列。所以题主要做一件事情是建立一个俗称的Catalog，用来存放所有表的定义，主要是表名、列名和列的数据类型。传统数据库一个有意思的地方是这个Catalog也是存放在自己里面的，有点类似编译器的自举。常见的那些sys.tables、sys.columns都是这样的Catalog系统表。

> 进行一些初步的、逻辑层面的优化。常见的有：将where条件下移到子查询(Predicate Pushdown)、常量表达式替换为计算后的结果(Const Folding)、去除子查询里不必要的列、去除总为真的filter、去除总为假的filter对应的子句等等。

> 这一阶段产生的结果称为Logical
  Plan，它表达了逻辑上用户想要查询什么，而已经逐渐开始脱离最初的表达形式（原始的语句）。比如在这一阶段子查询已经不再是一个嵌套的SelectStmt，而变成了一个join的树，而where条件也被放在了经过优化之后的各个位置上。
  
[如果要做sql的一个解析器，已经生成ast了，接下来要怎么做呢](https://www.zhihu.com/question/53617196/answer/145772157)

### TableStats

1. selectivity(选择性)

> **描述列值数据分布的重要属性**

选择性 = 基数 / 总行数

通过上式可以看出，低选择性意味着列值没有太大的变化，也意味着不适合做索引。

[数据库索引和选择性的关系](https://my.oschina.net/gooke/blog/678673)

2. cardinality(基数)

对目标SQL的某个具体执行步骤的执行结果所包含记录数的估算值

基数 = 总行数 * 选择性

> 在查询优化中用于表征一个表与另一个表的关系** ?

> 如果是单一索引，就是唯一值的个数，如果是复合索引，就是唯一组合的个数 ?

[关于索引cardinality的知识](https://blog.csdn.net/shi_yi_fei/article/details/51659364)

[What is cardinality in Databases?](https://stackoverflow.com/questions/10621077/what-is-cardinality-in-databases)

[Query Tuning Fundamentals: Density, Predicates, Selectivity, and Cardinality](https://docs.microsoft.com/zh-cn/archive/blogs/bartd/query-tuning-fundamentals-density-predicates-selectivity-and-cardinality)

### intHistogram

1. Java的运算精度问题
```java
public class IntHistogram {
    // some code here
    private double estimateGreaterThan (int v) {
        // some code here
        for (int i = bucket+1; i < this.histogram.length; i++) {
            System.out.println(this.histogram[i]/this.ntups); // 0
            System.out.println(Double.valueOf(this.histogram[i]/ this.ntups)); // 0.0
            System.out.println(Double.valueOf(this.histogram[i]) / this.ntups); // 0.6
            System.out.println(this.histogram[i]/ Double.valueOf(this.ntups)); // 0.6   
        }
    }   
}
```

当时错误地采用了第一种(隐式转化)和第二种(应该过程就用double参与，而不是结果转为double)方法

[Java类型转换: int转double](https://www.jianshu.com/p/919cd038db1b)

2. 单测不能发现所有错误

该类中有一变量定义未使用，但是运行该文件的单测并没有错误，反而是下一个与该类关联的`TableStatsTest`报错了

### 其他

1. Unhandled exception

[Java中出现Unhandled exception的原因](https://blog.csdn.net/qa275267067/article/details/81477929)

2. java处理字符串

[String,StringBuffer与StringBuilder的区别](https://blog.csdn.net/u011702479/article/details/82262823)

3. 对table scan, index scan和index seek的介绍(解释的非常好)

[Query Optimization：Scan Seek](https://blog.csdn.net/qq_26937525/article/details/54377450)

4. 堆表和聚集索引表

[[翻译] 聚集索引表 VS 堆表](https://www.cnblogs.com/kerrycode/p/3923525.html)

5. 一些关于数据访问方式，表连接的介绍

[SQL Server 优化器内幕（上篇）](https://zhuanlan.zhihu.com/p/39446213)

[SQL优化器原理 - Join重排](https://www.sypopo.com/post/745gXP3EoR/)

[How does a relational database work](http://coding-geek.com/how-databases-work/)