<?php
/**
 * @author: yevgen
 * @date: 20.01.17
 */

namespace YevhenHrytsai\JobQueue\Redis;


use Predis\ClientInterface;

class Sequence {
	/**
	 * @var ClientInterface
	 */
	private $client;
	/**
	 * @var string
	 */
	private $name;

	/**
	 * Sequence constructor.
	 * @param ClientInterface $client
	 * @param string $name
	 */
	public function __construct(ClientInterface $client, $name)
	{
		$this->client = $client;
		$this->name = $name;
	}

	public function nextValue()
	{
		return $this->client->incr($this->name);
	}
}