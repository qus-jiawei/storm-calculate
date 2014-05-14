storm-realtime
==============

本项目是一个简单的构建于storm之上的简单统计PV和UV的框架。

使用kafka分布式消息队列做数据源,业务方把数据推送到消息队列。
storm 从kafka获取数据经过计算将结果推送到mysql。
