Chainpal是一个聊天机器人，它底层用UDP协议。聊天的一来一回叫做一个Round，客户端定期发送的心跳包中会包含客户端所期待的Round ID，如果旧Round还没有完成，但是客户端已经在期待新Round了，那么服务器端就会放弃旧Round，切换到新Round上来工作。服务器端返回的心跳包中会包含同样的Round ID。

除了心跳包之外的其他UDP包，都会包含Round ID、Part ID和Part Count。Part Count指这个消息总共有几个Part，Part ID表示当前包是第几个Part。

服务器端必须收集到完整的消息后，才会响应。如果客户端迟迟没有收到服务器的反馈，不管是因为发送请求时出现了问题，还是发送响应时出现了问题，只要是超时了，就放弃当前的Round，开启一个新Round，然后重新发送一模一样的请求。

Mini-Daemon（中文名：迷你精灵）是用WASM实现的超轻量级的守护进程，比Docker还要轻很多。它同Docker一样，可以被托管在云上。
每个Mini-Daemon所得到的资源非常有限，包括一个磁盘目录，可以读写文件，若干UDP连接，专属的一个“IP地址+端口”。使用“IP地址+端口”可以唯一确定一个Mini-Daemon，MD相互之间依靠“IP地址+端口”来相互识别和通讯。Host帮助HD进行DDoS防护，设置白名单、黑名单等；Host帮助HD记录它的公钥，供外部查询。
MD进行通讯、计算、存储（SSD、DRAM）都需要支付gas。
MD有两个重要的基础标准，MD-host-std定义了最少要支持哪些Host函数；MD-comm-std定义了MD相互之间通讯时，如何协商通讯密钥，如何用UDP Packet构造cbor消息，以及哪些类型的cbor消息应该得到支持。 
