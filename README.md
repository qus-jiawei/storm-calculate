storm-calculate
==============

本项目是一个简单的构建于storm之上的简单统计PV和UV的框架。

使用kafka分布式消息队列做数据源,业务方把数据推送到消息队列。

storm 从kafka获取数据经过计算将结果推送到mysql。

####计算拓扑图:

![](https://raw.github.com/qus-jiawei/storm-calculate/master/doc/storm-realtime.png)


####bolt介绍:

- PvBolt用于计算可直接累加的数据,采用随机订阅方式
 
- PvSumBolt用于汇总PvBolt的数据,采用按字段哈希订阅方式,推送到持久化层
  
- UvBolt用于计算需要通过制定的uid去重的数据，采用按字段哈希的订阅方式

- UvSumBolt用于汇总UvBolt的数据,采用按字段哈希的订阅方式,推送到持久化层。uid去重方式使用bloom过滤器来节省内存

####TODO :

如果某个结点倒掉内存中保存的用于去重的cache将会消失,需要考虑以下两个容错手段：

A.  将内存字段持久化到MC或者redis
B.  发送两份数据到流
