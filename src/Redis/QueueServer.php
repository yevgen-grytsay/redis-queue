<?php
/**
 * @author: yevgen
 * @date: 20.01.17
 */

namespace YevhenHrytsai\JobQueue\Redis;


use Predis\Client;

class QueueServer {
	const STATUS_READY = 'ready';
	const STATUS_PROCESSING = 'processing';
	const STATUS_ACKNOWLEDGED = 'ack';
	/**
	 * @var Client
	 */
	private $client;
	/**
	 * @var Sequence
	 */
	private $sequence;
	/**
	 * @var string
	 */
	private $messageStorage;

	/**
	 * Queue constructor.
	 * @param Client $client
	 * @param Sequence $sequence
	 * @param $messageStorage
	 */
	public function __construct(Client $client, Sequence $sequence, $messageStorage = 'messages')
	{
		$this->client = $client;
		$this->sequence = $sequence;
		$this->messageStorage = $messageStorage;
	}

	/**
	 * @param $queueName
	 * @param $payload
	 * @return int
	 */
	public function enqueue($queueName, $payload)
	{
		list($id, $message) = $this->createMessage($queueName, $payload);
		$this->client->transaction()
			->hmset('messages:'.$id, $message)
			->lpush($queueName, [$id])
			->exec();
		return $id;
	}

	/**
	 * @param $consumerId
	 * @param $queueName
	 * @return Consumer
	 */
	public function consumer($consumerId, $queueName)
	{
		return new Consumer($consumerId, $queueName, $this->messageStorage, $this->client);
	}

	/**
	 * @param $id
	 * @return string
	 */
	public function getMessageById($id)
	{
		return $this->client->get($this->key($id));
	}

	/**
	 * @param $id
	 * @return bool
	 */
	public function deleteMessageById($id)
	{
		return $this->client->del([$this->key($id)]) > 0;
	}

	/**
	 * @param $id
	 * @return string
	 */
	public function getStatusById($id)
	{
		return $this->client->hget($this->key($id), 'status');
	}

	/**
	 * @param $queueName
	 * @param $payload
	 * @return array
	 */
	private function createMessage($queueName, $payload)
	{
		$id = $this->sequence->nextValue();
		return [$id, ['id' => $id, 'queue' => $queueName, 'status' => self::STATUS_READY, 'payload' => $payload]];
	}

	/**
	 * @param $id
	 * @return string
	 */
	private function key($id)
	{
		return $this->messageStorage . ':' . $id;
	}
}