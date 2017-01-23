<?php
use PHPUnit\Framework\TestCase;
use YevhenHrytsai\JobQueue\Redis\Delivery;
use YevhenHrytsai\JobQueue\Redis\QueueServer;
use YevhenHrytsai\JobQueue\Redis\Sequence;

/**
 * @author: yevgen
 * @date: 21.01.17
 */
class QueueTest extends TestCase {
    private $payload = 'test_data';
    private $queueName = 'test_queue';
    private $sequenceName = 'test_seq';
    private $prefix = 'phpunit:';
    private $listKey;
    private $consumerId = 1;
    /**
     * @var QueueServer
     */
    private $queue;
    /**
     * @var \Predis\Client
     */
    private $client;

    protected function setUp()
    {
        $this->client = new \Predis\Client();
        $client = new \Predis\Client(null, ['prefix' => $this->prefix]);
        $this->queue = new QueueServer($client, new Sequence($client, $this->sequenceName));
        $this->listKey = $this->prefix . $this->queueName;
    }

    protected function tearDown()
    {
        $this->client->flushdb();
    }

    public function testEnqueuePayload()
    {
        $this->enqueue();
        $this->assertQueueLengthEquals(1);
    }

    public function testStoreHeaders()
    {
        $id = $this->enqueue();
        $expected = [
            'id' => ''.$id,
            'queue' => $this->queueName,
        ];
        $this->assertEquals($expected, $this->getHeaderById($id));
    }

    public function testConsume()
    {
        $this->enqueue();
        $this->assertEquals($this->payload, $this->pop()->getPayload());
        $this->assertQueueLengthEquals(0);
    }

    public function testDiscard()
    {
        $id = $this->enqueue();
        $this->queue->discardMessageById($id);
        $this->assertNull($this->pop());
        $this->assertQueueLengthEquals(0);
        $this->assertEquals(QueueServer::STATUS_DISCARDED, $this->client->hget($this->prefix.QueueServer::headerKey($id), 'status'));
    }

    public function testTerminator()
    {
        $message = $this->pop();
        $this->assertTrue($this->queue->isTerminator($message));
    }

    public function testRecover()
    {
        $this->enqueue();
        $this->client->rpoplpush($this->listKey, $this->prefix.QueueServer::unackedKey($this->queueName, $this->consumerId));
        $this->assertQueueLengthEquals(0);

        $this->recover();
        $this->assertEquals($this->payload, $this->pop()->getPayload());
        $this->assertQueueLengthEquals(0);
    }

    public function testStatus()
    {
        $id = $this->enqueue();
        $header = $this->getHeaderById($id);
        $this->assertFalse(array_key_exists('status', $header));

        $message = $this->pop();
        $this->assertEquals(QueueServer::STATUS_PROCESSING, $message->getStatus());
        $message->ack();
        $this->assertEquals(QueueServer::STATUS_ACKNOWLEDGED, $message->getStatus());
    }

    private function popMessage()
    {
        return $this->client->lpop($this->listKey);
    }

    private function getHeaderById($id)
    {
        return $this->client->hgetall($this->prefix.QueueServer::headerKey($id));
    }

    private function pop()
    {
        return $this->queue->pop($this->consumerId, $this->queueName);
    }

    private function enqueue()
    {
        return $this->queue->enqueue($this->queueName, $this->payload);
    }

    private function recover()
    {
        $this->queue->recover($this->consumerId, $this->queueName);
    }

    private function assertQueueLengthEquals($expected)
    {
        $this->assertEquals($expected, $this->client->llen($this->listKey));
    }
}