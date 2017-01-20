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
	 * @var string
	 */
	private $prefix;

	/**
	 * Queue constructor.
	 * @param Client $client
	 * @param Sequence $sequence
	 * @param string $prefix
	 */
	public function __construct(Client $client, Sequence $sequence, $prefix)
	{
		$this->client = $client;
		$this->sequence = $sequence;
		$this->prefix = $prefix;
	}

	private function name($name)
	{
		return sprintf('%s:%s', $this->prefix, $name);
	}

	public function getMessageById($id)
	{
		return $this->client->get($id);
	}

	/**
	 * @param $name
	 * @return Queue
	 */
	public function queue($name)
	{
		return new Queue($this, $name);
	}

	/**
	 * @param $queueName
	 * @param $payload
	 * @return QueuedMessage
	 */
	public function enqueue($queueName, $payload)
	{
		list($id, $message) = $this->createMessage($payload);
		//TODO: this is not transactional and it's ok? check!
		$this->client->set($this->name('messages:'.$id), $message);
		$this->client->rpush($this->name($queueName), [$id]);

		return new QueuedMessage($this, $id);
	}

	public function pop($queueName)
	{
		//TODO: rpoplpush
		do {
			$id = $this->client->lpop($this->name($queueName));
			if (!$id) return false;
			$message = $this->client->get($this->name('messages:'.$id));
		} while(!$message);
		$this->client->del($this->name('messages:'.$id));
		return $message;
	}

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
		return $this->client->del($this->name('messages:'.$id)) > 0;
	}
}