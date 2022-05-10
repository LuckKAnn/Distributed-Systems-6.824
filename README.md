# Distributed-Systems-6.824




# 踩坑记录
* 关于类中属性，首字母大小写的问题。如果不进行大写，在非同文件下是不能够被访问的，但是却不会报错，似乎还是初始值为0







# lab1
* lab1中需要注意的是，socket的使用。我一开始使用了TCP进行连接，结果在最后一个crash的测试里面一直卡着，后来看了shell脚本才发现，似乎crash和Unxi方式的网络连接有关
* 只有在Unix之下，才会创建多个worker去进行错误重试，否则的话仅仅创建一个worker，如果这个worker crash了，那么自然这个测试就无法通过。

* 测试的shell脚本，以及crash.go似乎并不能覆盖所有的情况。根据我这一版的代码，由于没有采用临时文件，每次map任务都是直接写到中间文件中的，根据
* crash.go的代码，其至多睡眠的时间是10s，(在一些条件下会直接exit)但是如果实际情况下，程序假如卡住了，睡眠了15s，但是其还在运行，而由于崩溃恢复
* 该任务会交给其他worker来执行，这样如果原来的任务也执行完了，新任务也执行完了，都写入了文件，那么内容就产生了重复，也就是多了一部分。
  * 解决方法的话，是不是应该把数据返回到master来进行提交?只有未超时的任务才能够提交进入文件