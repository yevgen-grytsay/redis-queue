<?php
/**
 * @author: yevgen
 * @date: 20.01.17
 */

namespace YevhenHrytsai\JobQueue\Redis;


use Predis\Client;

class Sequence {
	/**
	 * @var Client
	 */
	private $client;
	/**
	 * @var string
	 */
	private $name;

	/**
	 * Sequence constructor.
	 * @param Client $client
	 * @param string $name
	 */
	public function __construct(Client $client, $name)
	{
		$this->client = $client;
		$this->name = $name;
	}

	public function nextValue() {
		return $this->client->incr($this->name);
	}
}