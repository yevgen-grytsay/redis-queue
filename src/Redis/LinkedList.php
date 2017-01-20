<?php
/**
 * @author: yevgen
 * @date: 20.01.17
 */
namespace YevhenHrytsai\JobQueue\Redis;


use Predis\Client;

class LinkedList {
	/**
	 * @var Client
	 */
	private $client;
	/**
	 * @var string
	 */
	private $key;

	/**
	 * YevhenHrytsai\JobQueue\Redis\RedisList constructor.
	 * @param Client $client
	 * @param string $key
	 */
	public function __construct(Client $client, $key)
	{
		$this->client = $client;
		$this->key = $key;
	}

	public function append(array $values)
	{
		return $this->client->rpush($this->key, $values);
	}

	public function head()
	{
		return $this->client->lpop($this->key);
	}

	public function tail()
	{
		return $this->client->rpop($this->key);
	}

	public function moveTo(LinkedList $dest)
	{
		return $this->client->rpoplpush($this->key, $dest->key);
	}
}