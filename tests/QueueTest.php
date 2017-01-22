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
		$expected = [
			'id' => ''.$id,
			'payload' => $this->payload
		];
		$this->assertEquals($expected, json_decode($this->popMessage(), true));
	}

	public function testStoreMessage()
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
		$this->assertEquals(0, $this->client->llen($this->listKey));
	}

	public function testRecover()
	{
		$id = $this->enqueue();
		$this->client->rpoplpush($this->listKey, $this->listKey.':unacked:'.$id);
		$this->assertEquals(0, $this->client->llen($this->listKey));

		$this->recover();
		$this->assertEquals($this->payload, $this->pop()->getPayload());
		$this->assertEquals(0, $this->client->llen($this->listKey));
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
		return $this->client->hgetall($this->hashKey.':'.$id);
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
}