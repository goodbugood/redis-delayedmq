<?php

namespace Tests\Shali\RedisDelayedMQ;

use Shali\RedisDelayedMQ\Server;
use PHPUnit\Framework\TestCase;

class ServerTest extends TestCase
{
    /**
     * @var Server
     */
    private $server;

    protected function setUp()
    {
        parent::setUp();
        $redis = new \Redis();
        $host = $_ENV['REDIS_HOST'];
        $port = $_ENV['REDIS_PORT'];
        self::assertTrue($redis->connect($host, $port), '连接redis失败');
        $redis->select($_ENV['REDIS_DB_NUM']);
        $this->server = new Server($redis);
        $this->server->attachWatchQueue($_ENV['REDIS_DELAY_QUEUE_NAME']);
    }

    public function testRun()
    {
        // 常驻内存
        while (true) {
            // reserved -> ready
            $this->server->run();
        }
    }
}
