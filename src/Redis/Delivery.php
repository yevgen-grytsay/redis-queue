<?php
/**
 * @author: yevgen
 * @date: 21.01.17
 */

namespace YevhenHrytsai\JobQueue\Redis;


use Predis\Client;

class Delivery {
	/**
	 * @var string
	 */
	private $payload;
	/**
	 * @var string
	 */
	private $key;
	/**
	 * @var string
	 */
	private $unackedPool;
	/**
	 * @var Client
	 */
	private $client;

	/**
	 * Delivery constructor.
	 * @param string $payload
	 * @param string $key
	 * @param string $unackedPool
	 * @param Client $client
	 */
	public function __construct($payload, $key, $unackedPool, Client $client)
	{
		$this->payload = $payload;
		$this->key = $key;
		$this->unackedPool = $unackedPool;
		$this->client = $client;
	}

	public function ack()
	{
		$this->client->transaction()
			->hset($this->key, 'status', QueueServer::STATUS_ACKNOWLEDGED)
			->hdel($this->key, ['payload'])
			->rpop($this->unackedPool) //TODO use lrem
			->exec();
	}

	/**
	 * @return string
	 */
	public function getPayload()
	{
		return $this->payload;
	}

	/**
	 * @param $status
	 */
	public function updateStatus($status)
	{
		$this->client->hset($this->key, 'status', $status);
	}

	/**
	 * @return string
	 */
	public function getStatus()
	{
		return $this->client->hget($this->key, 'status');
	}
}