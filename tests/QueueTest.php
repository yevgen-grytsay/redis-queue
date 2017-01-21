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
	private $hashKey;
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
		$this->hashKey = $this->prefix . 'messages';
	}

	public function testEnqueue()
	{
		$this->queue->enqueue($this->queueName, $this->payload);
		$this->assertEquals(1, $this->client->llen($this->listKey));
	}

	public function testStoreMessage()
	{
		$id = $this->queue->enqueue($this->queueName, $this->payload);
		$expected = [
			'id' => ''.$id,
			'queue' => $this->queueName,
			'payload' => $this->payload,
			'status' => QueueServer::STATUS_READY
		];
		$this->assertEquals($expected, $this->client->hgetall($this->hashKey.':'.$id));
	}

	public function testConsume()
	{
		$this->queue->enqueue($this->queueName, $this->payload);
		$actual = [];
		$this->queue->consumer(1, $this->queueName)->consume(function (Delivery $message) use (&$actual) {
			$actual = $message->getPayload();
		});
		$this->assertEquals($this->payload, $actual);
		$this->assertEquals(0, $this->client->llen($this->listKey));
	}

	public function testRecover()
	{
		$id = $this->queue->enqueue($this->queueName, $this->payload);
		$this->client->rpoplpush($this->listKey, $this->listKey.':unacked:'.$id);
		$this->assertEquals(0, $this->client->llen($this->listKey));
		$this->queue->consumer(1, $this->queueName)->consume(function (Delivery $message) use (&$actual) {
			$actual = $message->getPayload();
		});
		$this->assertEquals($this->payload, $actual);
		$this->assertEquals(0, $this->client->llen($this->listKey));
	}

	public function testStatus()
	{
		$id = $this->queue->enqueue($this->queueName, $this->payload);
		$this->assertEquals(QueueServer::STATUS_READY, $this->client->hget($this->hashKey.':'.$id, 'status'));
		$this->queue->consumer(1, $this->queueName)->consume(function (Delivery $message) use (&$actual) {
			$this->assertEquals(QueueServer::STATUS_PROCESSING, $message->getStatus());
			$message->ack();
			$this->assertEquals(QueueServer::STATUS_ACKNOWLEDGED, $message->getStatus());
		});

	}

	protected function tearDown()
	{
		$this->client->flushdb();
	}

}