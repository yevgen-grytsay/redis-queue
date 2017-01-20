<?php
/**
 * @author: yevgen
 * @date: 20.01.17
 */

namespace YevhenHrytsai\JobQueue\Redis;


class Queue {
	/**
	 * @var QueueServer
	 */
	private $server;
	/**
	 * @var string
	 */
	private $name;

	/**
	 * Queue constructor.
	 * @param QueueServer $server
	 * @param string $name
	 */
	public function __construct(QueueServer $server, $name)
	{
		$this->server = $server;
		$this->name = $name;
	}

	/**
	 * @param $payload
	 * @return QueuedMessage
	 */
	public function enqueue($payload)
	{
		return $this->server->enqueue($this->name, $payload);
	}

	/**
	 * @return bool|string
	 */
	public function pop()
	{
		return $this->server->pop($this->name);
	}
}