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
     * @param string $sPayload
     * @param int $iDelay
     * @return bool
     */
    public function put($sPayload, $iDelay = 0)
    {
        $iScore = time() + $iDelay;

        // -> ready
        return is_numeric($this->oRedis->zAdd($this->sReadyQueueName, $iScore, $sPayload));
    }

    /**
     * @param string $sPayload
     * @param int $iDelay
     * @return bool
     */
    public function release($sPayload, $iDelay = 0)
    {
        // reserved -> ready
        $this->delete($sPayload);

        return $this->put($sPayload, $iDelay);
    }

    public function bury($sPayload)
    {
        // reserved -> bury
        $this->delete($sPayload);
        $this->oRedis->zAdd($this->sBuriedQueueName, 0, $sPayload);

        return true;
    }

    public function kick($sPayload, $iDelay = 0)
    {
        // bury -> ready
        $this->oRedis->zDelete($this->sBuriedQueueName, $sPayload);

        return $this->put($sPayload, $iDelay);
    }

    /**
     * 获取一个待消费的任务
     * 暂不支持超时重发
     * @param int $iTimeout
     * @return string|null
     */
    public function reserve($iTimeout = 0)
    {
        $aData = $this->oRedis->zRangeByScore($this->sReadyQueueName, 0, time(), ['limit' => [0, 1]]);
        if (empty($aData)) {
            return null;
        }
        $sPayload = $aData[0];
        $iScore = time() + $iTimeout;
        // ready -> reserved
        $this->oRedis->zDelete($this->sReadyQueueName, $sPayload);
        $this->oRedis->zAdd($this->sReservedQueueName, $iScore, $sPayload);

        return $sPayload;
    }

    /**
     * 删除队列中的元素
     * @param string $sPayload
     * @return bool
     */
    public function delete($sPayload)
    {
        // reserved -> delete
        // return $this->oRedis->zRem($this->sReservedQueueName, $sPayload) > 0;
        return $this->oRedis->zDelete($this->sReservedQueueName, $sPayload) > 0;
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
}