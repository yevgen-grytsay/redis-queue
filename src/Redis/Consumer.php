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
	 * Consumer constructor.
	 * @param string $id
	 * @param string $queueName
	 * @param Client $client
	 */
	public function __construct($id, $queueName, Client $client)
	{
		$this->id = $id;
		$this->queueName = $queueName;
		$this->client = $client;
	}

	public function consume()
	{
		$unackedPool = $this->queueName . ':' . $this->id;
		$this->client->rpoplpush($unackedPool, $this->queueName);
		while (true) {
			$message = null;
			do {
				$id = $this->client->rpoplpush($this->queueName, $unackedPool);
				if (!$id) break;
				$message = $this->client->get('messages:'.$id);
				if (!$message) {
					//TODO use "rm" or "trim" command
					$this->client->rpop($unackedPool);
				}
			} while(!$message);
			if (!$message) {
				break;
			}

			yield $message;
			$this->client->transaction()
				->del('messages:'.$id)
				->rpop($unackedPool)
				->exec();
		}
	}
}