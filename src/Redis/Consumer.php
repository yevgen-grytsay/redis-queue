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
	 * @var string
	 */
	private $unackedPool;

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
		$this->unackedPool = $this->queue . ':unacked:' . $id;
	}

	/**
	 * @param callable $callback
	 */
	public function consume(callable $callback)
	{
		$this->recover();
		while (true) {
			$delivery = $this->pop();
			if (!$delivery) {
				break;
			}
			call_user_func($callback, $delivery);
		}
	}

	/**
	 * @param callable $callback
	 */
	public function consumeBlocking(callable $callback)
	{
		$this->recover();
		while (true) {
			call_user_func($callback, $this->pop(true));
		}
	}

	private function recover()
	{
		$this->client->rpoplpush($this->unackedPool, $this->queue);
	}

	/**
	 * @param bool $blocking
	 * @return null|Delivery
	 */
	private function pop($blocking = false)
	{
		$message = null;
		$client = $this->client;
		while(!$message) {
			$id = $blocking
				? $client->brpoplpush($this->queue, $this->unackedPool, 10)
				: $client->rpoplpush($this->queue, $this->unackedPool);
			if (!$id) break;
			$data = $this->findMessageBodyById($id);
			/* Message may be deleted before it will be processed */
			if (!$data) {
				$client->rpop($this->unackedPool);
			} else {
				$message = new Delivery($data['payload'], $this->messageKey($id), $this->unackedPool, $client);
				$message->updateStatus(QueueServer::STATUS_PROCESSING);
			}
		};

		return $message;
	}

	/**
	 * @param $id
	 * @return array
	 */
	private function findMessageBodyById($id)
	{
		return $this->client->hgetall($this->messageKey($id));
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