# 介绍

自定义Hive Hook（钩子函数），继承自org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext，通过拦截Hive执行引擎执行后返回给用户前的调用，获取任务执行的相关信息，并且分析用户执行的SQL语句，获取元数据信息和数据血缘关系，支持字段级别的血缘关系。信息收集后会发送至Kafka消息集群。

# 环境说明

支持版本

Hive 1.*

# 部署
1. 编译

   在项目根目录下执行mavne命令：

   mvn clean package

2. 部署

   将编译后的jar包放到集群中hive server所在机器的${HIVE_HOME}/auxlib目录下，

   例如：“/opt/cloudera/parcels/CDH/lib/hive/auxlib”

   如果auxlib目录不存在，在${HIVE_HOME}下新建auxlib目录。

3. 配置

   hive-site.xml中添加hook模块
   <property>
         <name>hive.exec.post.hooks</name>
         <value>org.billcc.hive.hook.HiveHook</value>
   </property>

4. Kafka配置

   配置kafka-application.properties属性文件，如下：

   #########  Kafka Configs  #########
   bootstrap.servers=ip1:port,ip2:port,ip3:port
   auto.commit.interval.ms=100
   enable.auto.commit=false
   session.timeout.ms=30000

   可以将文件提取到和jar包同级目录中，优先加载同级外部配置文件。