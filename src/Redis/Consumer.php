<?php
/**
 * @author: yevgen
 * @date: 20.01.17
 */

namespace YevhenHrytsai\JobQueue\Redis;


use Predis\Client;

class Consumer {
	/**
	 * @var string
	 */
	private $queue;
	/**
	 * @var Client
	 */
	private $client;
	/**
	 * @var string
	 */
	private $storageName;

	/**
	 * Consumer constructor.
	 * @param string $id
	 * @param string $queueName
	 * @param $storageName
	 * @param Client $client
	 */
	public function __construct($id, $queueName, $storageName, Client $client)
	{
		$this->queue = $queueName;
		$this->client = $client;
		$this->storageName = $storageName;
		$this->unackedPool = $this->queue . ':' . $id;
	}

	public function consume()
	{
		$this->recover();
		while (true) {
			list($id, $message) = $this->pop();
			if (!$message) {
				break;
			}
			yield $message;
			$this->ack($id);
		}
	}

	private function recover()
	{
		$this->client->rpoplpush($this->unackedPool, $this->queue);
	}

	private function pop()
	{
		$message = null;
		$client = $this->client;
		do {
			$id = $client->rpoplpush($this->queue, $this->unackedPool);
			$messageBodyKey = $this->messageKey($id);
			if (!$id) break;
			$message = $client->hgetall($messageBodyKey);
			if (!$message) {
				$client->rpop($this->unackedPool);
			}
			$client->hset($messageBodyKey, 'status', QueueServer::STATUS_PROCESSING);
		} while(!$message);
		return [$id, $message];
	}

	private function ack($id)
	{
		$this->client->transaction()
			->hset($this->messageKey($id), 'status', QueueServer::STATUS_ACKNOWLEDGED)
			->hdel($this->messageKey($id), ['payload'])
			->rpop($this->unackedPool)
			->exec();
	}

	/**
	 * @param $id
	 * @return string
	 */
	private function messageKey($id)
	{
		return $this->storageName . ':' . $id;
	}
}