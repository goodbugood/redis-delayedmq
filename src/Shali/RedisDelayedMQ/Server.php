<?php

namespace Shali\RedisDelayedMQ;

/**
 * 服务端实现延时队列自动的超时ack, 支持超时重发
 */
class Server
{
    /**
     * @var \Redis $oRedis
     */
    protected $oRedis;

    /**
     * @var string[] 监听的队列名称
     */
    protected $aWatchQueue = [];

    public function __construct(\Redis $oRedis)
    {
        $this->oRedis = $oRedis;
    }

    /**
     * 设置监听的延时队列
     * @param string[] $aQueue 监听的延时消息队列
     * @return self
     */
    public function watchQueue($aQueue)
    {
        foreach ($aQueue as $sQueueName) {
            $this->attachWatchQueue($sQueueName);
        }

        return $this;
    }

    /**
     * 追加监听超时的延时队列
     * @param string $sQueueName 队列名称
     * @return self
     */
    public function attachWatchQueue($sQueueName)
    {
        $sReservedQueueName = sprintf('%s_#RESERVED', $sQueueName);
        if (!in_array($sReservedQueueName, $this->aWatchQueue)) {
            $sReadyQueueName = sprintf('%s_#READY', $sQueueName);
            $this->aWatchQueue[$sReadyQueueName] = $sReservedQueueName;
        }

        return $this;
    }

    /**
     * 运行
     * @throws \InvalidArgumentException
     */
    public function run()
    {
        $this->handleTimeOut();
    }

    protected function haveTimeOutJob($sReservedQueueName)
    {
        return $this->oRedis->zCount($sReservedQueueName, 0, time()) > 0;
    }

    protected function handleTimeOut()
    {
        $releaseScript = <<<LUA
local reservedQueueName = KEYS[1]
local readyQueueName = KEYS[2]
local score = ARGV[1]
local resultArray = redis.call('zrangebyscore', reservedQueueName, 0, score, 'limit', 0, 1)
if #resultArray ~= 1 then
    return -1
end
local payload = resultArray[1]
if redis.call('zrem', reservedQueueName, payload) ~= 1 then
    return -2
end
if false == redis.call('zrank', readyQueueName, payload) then
    if redis.call('zadd', readyQueueName, score, payload) ~= 1 then
        return -3
    end
end
return payload
LUA;

        foreach ($this->aWatchQueue as $sReadyQueueName => $sReservedQueueName) {
            while ($this->haveTimeOutJob($sReservedQueueName)) {
                $iTimeout = time();
                // reserved => ready
                $res = $this->oRedis->eval(
                    $releaseScript,
                    [
                        $sReservedQueueName,
                        $sReadyQueueName,
                        $iTimeout,
                    ],
                    2
                );
                if (false === $res) {
                    throw new \InvalidArgumentException($this->oRedis->getLastError());
                } elseif (in_array($res, [-1, -2, -3,], true)) {
                    // 插入失败
                    continue;
                }

                $sPayload = $res;
            }
        }
    }
}