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
	private $id;
	/**
	 * @var string
	 */
	private $queueName;
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
		$this->id = $id;
		$this->queueName = $queueName;
		$this->client = $client;
		$this->storageName = $storageName;
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
		$this->client->rpoplpush($this->unackedPoolKey(), $this->queueName);
	}

	private function pop()
	{
		$message = null;
		$unackedPool = $this->unackedPoolKey();
		do {
			$id = $this->client->rpoplpush($this->queueName, $unackedPool);
			$messageBodyKey = $this->messageKey($id);
			if (!$id) break;
			$message = $this->client->hgetall($messageBodyKey);
			if (!$message) {
				$this->client->rpop($unackedPool);
			}
			$this->client->hset($messageBodyKey, 'status', QueueServer::STATUS_PROCESSING);
		} while(!$message);
		return [$id, $message];
	}

	private function ack($id)
	{
		$this->client->transaction()
			->hset($this->messageKey($id), 'status', QueueServer::STATUS_ACKNOWLEDGED)
			->hdel($this->messageKey($id), ['payload'])
			->rpop($this->unackedPoolKey())
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

	/**
	 * @return string
	 */
	private function unackedPoolKey()
	{
		return $this->queueName . ':' . $this->id;
	}
}