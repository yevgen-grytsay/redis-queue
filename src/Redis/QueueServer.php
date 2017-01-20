<?php
/**
 * @author: yevgen
 * @date: 20.01.17
 */

namespace YevhenHrytsai\JobQueue\Redis;


use Predis\Client;

class QueueServer {
	/**
	 * @var Client
	 */
	private $client;
	/**
	 * @var Sequence
	 */
	private $sequence;

	/**
	 * Queue constructor.
	 * @param Client $client
	 * @param Sequence $sequence
	 */
	public function __construct(Client $client, Sequence $sequence)
	{
		$this->client = $client;
		$this->sequence = $sequence;
	}

	/**
	 * @param $id
	 * @return string
	 */
	public function getMessageById($id)
	{
		return $this->client->get($id);
	}

	/**
	 * @param $name
	 * @return OutChannel
	 */
	public function queue($name)
	{
		return new OutChannel($this, $name);
	}

	/**
	 * @param $queueName
	 * @param $payload
	 * @return QueuedMessage
	 */
	public function enqueue($queueName, $payload)
	{
		list($id, $message) = $this->createMessage($payload);
		$this->client->transaction()
			->set('messages:'.$id, $message)
			->lpush($queueName, [$id])
			->exec();
		return new QueuedMessage($this, $id);
	}

	/**
	 * @param $consumerId
	 * @param $queueName
	 * @return Consumer
	 */
	public function consumer($consumerId, $queueName)
	{
		return new Consumer($consumerId, $queueName, $this->client);
	}

	/**
	 * @param $payload
	 * @return array
	 */
	private function createMessage($payload)
	{
		$id = $this->sequence->nextValue();
		return [$id, json_encode(['id' => $id, 'status' => 'ready', 'payload' => $payload])];
	}

	/**
	 * @param $id
	 * @return bool
	 */
	public function deleteMessageById($id)
	{
		return $this->client->del('messages:'.$id) > 0;
	}
}