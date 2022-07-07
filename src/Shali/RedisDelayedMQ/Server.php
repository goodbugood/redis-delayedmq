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
        foreach ($this->aWatchQueue as $sReadyQueueName => $sReservedQueueName) {
            while ($this->haveTimeOutJob($sReservedQueueName)) {
                $aData = $this->oRedis->zRangeByScore($sReservedQueueName, 0, time(), ['limit' => [0, 1]]);
                if (empty($aData)) {
                    continue;
                }
                $sPayload = $aData[0];
                // reserved => ready
                $this->oRedis->zDelete($sReservedQueueName, $sPayload);
                $this->oRedis->zAdd($sReadyQueueName, time(), $sPayload);
            }
        }
    }
}