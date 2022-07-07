# 基于redis有序集合的延时消息队列

最小，最简洁的延时消息队列，支持ACK。

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
if ($client->haveReadyJob()) {
    // 获取延时到期的任务
    $task = $client->reserve();
    // 处理任务...
    // 消费者：任务处理成功，将其从队列移除
    $client->delete($task);
}

# 服务端使用
$server = new \Shali\RedisDelayedMQ\Server($redis);
while (true) {
    // 将处理超时的任务，再次丢进去处理
    $server->run();
}
```