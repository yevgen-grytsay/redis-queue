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
		$this->hashKey = $this->prefix . 'messages';
	}

	protected function tearDown()
	{
		$this->client->flushdb();
	}

	public function testEnqueue()
	{
		$id = $this->enqueue();
		$this->assertEquals($id, $this->client->lpop($this->listKey));
	}

	public function testStoreMessage()
	{
		$id = $this->enqueue();
		$expected = [
			'id' => ''.$id,
			'queue' => $this->queueName,
			'status' => QueueServer::STATUS_READY,
			'payload' => $this->payload
		];
		$this->assertEquals($expected, $this->getMessageById($id));
	}

	public function testConsume()
	{
		$id = $this->enqueue();
		$actual = [];
		$this->consume(function (Delivery $message) use (&$actual) {
			$actual = $message->getPayload();
		});
		$this->assertPayload($id);
		$this->assertEquals(0, $this->client->llen($this->listKey));
	}

	public function testRecover()
	{
		$id = $this->enqueue();
		$this->client->rpoplpush($this->listKey, $this->listKey.':unacked:'.$id);
		$this->assertEquals(0, $this->client->llen($this->listKey));
		$this->consume(function (Delivery $message) use (&$actual) {
			$actual = $message->getPayload();
		});
		$this->assertEquals($this->payload, $actual);
		$this->assertEquals(0, $this->client->llen($this->listKey));
	}

	public function testStatus()
	{
		$id = $this->enqueue();
		$this->assertStatus($id, QueueServer::STATUS_READY);
		$this->consume(function (Delivery $message) use (&$actual) {
			$this->assertEquals(QueueServer::STATUS_PROCESSING, $message->getStatus());
			$message->ack();
			$this->assertEquals(QueueServer::STATUS_ACKNOWLEDGED, $message->getStatus());
		});
	}

	private function assertPayload($id)
	{
		$this->assertEquals($this->payload, $this->getMessageFieldById($id, 'payload'));
	}

	private function assertStatus($id, $expected)
	{
		$this->assertEquals($expected, $this->getMessageFieldById($id, 'status'));
	}

	private function getMessageFieldById($id, $field)
	{
		return $this->client->hgetall($this->hashKey.':'.$id)[$field];
	}

	private function getMessageById($id)
	{
		return $this->client->hgetall($this->hashKey.':'.$id);
	}

	private function consume(callable $callback)
	{
		$this->queue->consumer($this->consumerId, $this->queueName)->consume($callback);
	}

	private function enqueue()
	{
		return $this->queue->enqueue($this->queueName, $this->payload);
	}
}