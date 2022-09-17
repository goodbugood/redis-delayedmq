# 基于redis有序集合的延时消息队列

最小，最简洁的延时消息队列，支持ACK。
支持分布式，高并发的延时消息队列。线程安全。

## 消息状态流转

```
   put            reserve               delete
  -----> [READY] ---------> [RESERVED] --------> *poof*
```

更多的信息

```
   put with delay               release with delay
  ----------------> [DELAYED] <------------.
                        |                   |
                        | (time passes)     |
                        |                   |
   put                  v     reserve       |       delete
  -----------------> [READY] ---------> [RESERVED] --------> *poof*
                       ^  ^                |  |
                       |   \  release      |  |
                       |    `-------------'   |
                       |                      |
                       | kick                 |
                       |                      |
                       |       bury           |
                    [BURIED] <---------------'
                       |
                       |  delete
                        `--------> *poof*
```

参考 Beanstalkd

Beanstalk，一个高性能、轻量级的分布式内存队列系统，最初设计的目的是想通过后台异步执行耗时的任务来降低高容量 Web 应用系统的页面访问延迟，支持过有 9.5 million 用户的 Facebook Causes 应用。

- [Beanstalkd 官网](https://beanstalkd.github.io/)
- [Beanstalkd 中文协议](https://github.com/beanstalkd/beanstalkd/blob/master/doc/protocol.zh-CN.md)

## 小试牛刀

```php
$host = '127.0.0.1';
$port = 6379;
$db = 6;
$queue_name = 'close_order';
$redis = new \Redis();
$redis->connect($host, $port);
$redis->select($db);

# 客户端使用
$client = \Shali\RedisDelayedMQ\Client::make($redis);
$client->useQueue($queue_name);
$order = 'pay20210509050607';
// 生产者：支付订单10分钟未支付，则关闭
$client->put($order, 10 * 60);
// 消费者：获取待检查关闭的订单
if ($client->haveReadyJob()) {// 双重检查：1
    // 获取延时到期的任务，默认获取的任务不指定 ttr
    $task = $client->reserve();
    if (null === $task) {// 双重检查：2
        return;
    }
    // 处理任务...
    if (true) {
        // 消费者：任务处理成功，将其从队列移除
        $client->delete($task);
    } else {
        // 任务处理不达预期，延时 10s 后再执行
        $client->release($task, 10);
    }
}

# 只有服务端运行起来，才支持超时重发
$server = new \Shali\RedisDelayedMQ\Server($redis);
while (true) {
    // 将处理超时的任务，再次丢进去处理
    $server->run();
}
```

### 双重检查

为了防止检查存在可消费任务时，去获取任务时，并发导致的任务不存在问题，我们使用双重检查来避免。