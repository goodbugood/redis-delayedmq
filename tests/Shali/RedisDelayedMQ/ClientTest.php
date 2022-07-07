<?php

namespace Tests\Shali\RedisDelayedMQ;

use PHPUnit\Framework\TestCase;
use Shali\RedisDelayedMQ\Client;

class ClientTest extends TestCase
{
    /**
     * @var Client
     */
    private static $delayedMQClient;

    /**
     * @var string 任务订单
     */
    private static $task = '订单20220707';

    public static function setUpBeforeClass()
    {
        parent::setUpBeforeClass();
        $redis = new \Redis();
        $host = $_ENV['REDIS_HOST'];
        $port = $_ENV['REDIS_PORT'];
        self::assertTrue($redis->connect($host, $port), '连接redis失败');
        self::$delayedMQClient = Client::make($redis);
        self::$delayedMQClient->selectDb($_ENV['REDIS_DB_NUM']);
        self::$delayedMQClient->useQueue($_ENV['REDIS_DELAY_QUEUE_NAME']);
    }

    protected function setUp()
    {
        parent::setUp();
    }

    public function testMake()
    {
        self::assertTrue(self::$delayedMQClient instanceof Client, '实例化延时消息队列客户端失败');
    }

    /**
     * @test 测试生成延时任务
     */
    public function put()
    {
        self::assertTrue(self::$delayedMQClient->put(self::$task, 3), '塞入延时任务失败');
    }

    /**
     * @test 测试消费延时任务
     * @depends put
     */
    public function reserve()
    {
        // 1. 首先检查是否有延时任务到时间了，需要被消费
        self::assertFalse(self::$delayedMQClient->haveReadyJob(), '延时任务提前出来了');
        sleep(3);
        // 2. 有任务到时间了，可以被消费了
        self::assertTrue(self::$delayedMQClient->haveReadyJob(), '延时任务超时也没出来');
        $task = self::$delayedMQClient->reserve();
        self::assertNotNull($task, '未获取到数据');

        return $task;
    }

    /**
     * @test 成功消费的任务，将其删除
     * @depends reserve
     */
    public function delete($task)
    {
        self::assertTrue(self::$delayedMQClient->delete($task));
    }

    /**
     * @test 消费失败的任务，将其再次塞入延时队列
     * @depends reserve
     */
    public function release($task)
    {
        // 消费失败，将任务再次放入延时队列，延时3秒执行
        self::assertTrue(self::$delayedMQClient->release($task, 5), '再次延时任务失败');
    }

    /**
     * @test 测试消费任务时，遇到争议的任务，就先留存下来
     * @depends reserve
     */
    public function bury()
    {
        $task = self::$task;
        self::assertTrue(self::$delayedMQClient->bury($task), '留存任务失败');

        return $task;
    }

    /**
     * @test 争议的任务，解决了争议，决定再丢回延时队列继续处理
     * @depends bury
     */
    public function kick($task)
    {
        self::assertTrue(self::$delayedMQClient->kick($task, 10));
    }

    /**
     * @test 测试获取统计信息
     */
    public function getStats()
    {
        $data = self::$delayedMQClient->getStats();
        self::assertTrue(is_array($data), '返回数据格式不对');
        self::assertArrayHasKey('currentJobsDelayed', $data, '缺失延时任务数');
    }

    protected function tearDown()
    {
        parent::tearDown();
    }

    public static function tearDownAfterClass()
    {
        parent::tearDownAfterClass();
        // 清理数据
        self::$delayedMQClient->delete(self::$task);
        self::$delayedMQClient->quit();
    }
}
