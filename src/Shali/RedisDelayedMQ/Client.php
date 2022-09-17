<?php

namespace Shali\RedisDelayedMQ;

class Client
{
    /**
     * @var self
     */
    private static $oInstance;

    /**
     * @var \Redis $oRedis
     */
    private $oRedis;

    /**
     * @var string 待消费的队列
     */
    private $sReadyQueueName;

    /**
     * @var string 消费中的队列
     */
    private $sReservedQueueName;

    /**
     * @var string 保留任务队列
     */
    private $sBuriedQueueName;

    private function __construct()
    {
    }

    private function __clone()
    {
    }

    /**
     * @param \Redis $oRedis
     * @return self
     */
    public static function make($oRedis)
    {
        if (is_null(self::$oInstance)) {
            self::$oInstance = new self();
        }
        self::$oInstance->oRedis = $oRedis;

        return self::$oInstance;
    }

    /**
     * 选库
     * @param int $iDbNum
     * @return self
     */
    public function selectDb($iDbNum)
    {
        $this->oRedis->select($iDbNum);

        return $this;
    }

    /**
     * 使用的队列的名称
     * @param string $sQueueName
     * @return self
     */
    public function useQueue($sQueueName)
    {
        $this->sReadyQueueName = sprintf('%s_#READY', $sQueueName);
        $this->sReservedQueueName = sprintf('%s_#RESERVED', $sQueueName);
        $this->sBuriedQueueName = sprintf('%s_#BURIED', $sQueueName);

        return $this;
    }

    /**
     * element -> ready
     * @param string $sPayload
     * @param int $iDelay
     * @return bool
     */
    public function put($sPayload, $iDelay = 0)
    {
        $iScore = time() + $iDelay;

        // 添加存在的元素返回 0，但是会更新 score 从而影响顺序，所以认为添加成功
        return $this->oRedis->zAdd($this->sReadyQueueName, $iScore, $sPayload) >= 0;
    }

    /**
     * reserved => ready
     * @param string $sPayload
     * @param int $iDelay
     * @return bool
     */
    public function release($sPayload, $iDelay = 0)
    {
        $iTimeout = time() + $iDelay;
        return $this->atomRemAdd($this->sReservedQueueName, $this->sReadyQueueName, $sPayload, $iTimeout);
    }

    /**
     * reserved => buried
     * @param string $sPayload
     * @return bool
     */
    public function bury($sPayload)
    {
        return $this->atomRemAdd($this->sReservedQueueName, $this->sBuriedQueueName, $sPayload, time());
    }

    /**
     * buried => ready
     * @param string $sPayload
     * @param int $iDelay
     * @return bool
     */
    public function kick($sPayload, $iDelay = 0)
    {
        $iTimeout = time() + $iDelay;
        return $this->atomRemAdd($this->sBuriedQueueName, $this->sReadyQueueName, $sPayload, $iTimeout);
    }

    /**
     * ready => reserved
     * 获取一个待消费的任务
     * 暂不支持超时重发
     * @param int $iTTR ttr（time to run）最大执行时间，单位秒，超时后，重新释放供其他消费者使用，0 永不超时
     * @return string|null
     */
    public function reserve($iTTR = 0)
    {
        $reserveScript = <<<'LUA'
local readyQueueName = KEYS[1]
local reservedQueueName = KEYS[2]
local score1 = ARGV[1]
local score2 = ARGV[2]
local resultArray = redis.call('zrangebyscore', readyQueueName, 0, score1, 'limit', 0, 1)
if #resultArray ~= 1 then
    return -1
end
local payload = resultArray[1]
if redis.call('zrem', readyQueueName, payload) ~= 1 then
    return -2
end
if false == redis.call('zrank', reservedQueueName, payload) then
    if redis.call('zadd', reservedQueueName, score2, payload) ~= 1 then
        return -3
    end
end
return payload
LUA;
        $iNow = time();
        $iScore = 0 === $iTTR ? -1 : $iNow + $iTTR;
        $res = $this->oRedis->eval(
            $reserveScript,
            [
                $this->sReadyQueueName,
                $this->sReservedQueueName,
                $iNow,
                $iScore,
            ],
            2
        );
        if (false === $res) {
            // 兼容旧的调用，暂不抛异常 $this->oRedis->getLastError()
            return null;
        } elseif (in_array($res, [-1, -2, -3,], true)) {
            return null;
        }
        $sPayload = $res;

        return $sPayload;
    }

    /**
     * reserved -> delete
     * 删除队列中的元素
     * @param string $sPayload
     * @return bool
     */
    public function delete($sPayload)
    {
        return $this->oRedis->zRem($this->sReservedQueueName, $sPayload) > 0;
    }

    /**
     * 检查当前是否有要任务要处理
     * @return bool
     */
    public function haveReadyJob()
    {
        return $this->oRedis->zCount($this->sReadyQueueName, 0, time()) > 0;
    }

    /**
     * 获取当前延时消息队列统计信息
     * @return array
     */
    public function getStats()
    {
        $iJobNum = $this->oRedis->zCard($this->sReadyQueueName);
        $iReadyNum = $this->oRedis->zCount($this->sReadyQueueName, 0, time());
        $iDelayedNum = $iJobNum - $iReadyNum;
        return [
            'currentJobsDelayed' => $iDelayedNum,// 延时任务数
            'currentJobsReady' => $iReadyNum,// 待处理任务数
            'currentJobsReserved' => $this->oRedis->zCard($this->sReservedQueueName),// 处理中的任务数
        ];
    }

    public function quit()
    {
        // 'close' is available starting with 5.6 PHP version
        if (PHP_VERSION_ID > 50600) {
            $this->oRedis->close();
        }
    }

    /**
     * 从旧队列中移除元素，并同时从新队列中添加元素，原子操作
     * @param string $sOldQueueName
     * @param string $sNewQueueName
     * @param string $sPayload
     * @param int $iTimeout
     * @return bool
     */
    private function atomRemAdd($sOldQueueName, $sNewQueueName, $sPayload, $iTimeout)
    {
        $luaScript = <<<'LUA'
local remZSetName = KEYS[1]
local addZSetName = KEYS[2]
local score = ARGV[1]
local payload = ARGV[2]
if redis.call('zrem', remZSetName, payload) ~= 1 then
    return -1
end
if false == redis.call('zrank', addZSetName, payload) then
    if redis.call('zadd', addZSetName, score, payload) ~= 1 then
        return -2
    end
end
return 1
LUA;
        $res = $this->oRedis->eval(
            $luaScript,
            [
                $sOldQueueName,
                $sNewQueueName,
                $iTimeout,
                $sPayload,
            ],
            2
        );
        if (false === $res) {
            return false;
        }

        return 1 === $res;
    }
}
