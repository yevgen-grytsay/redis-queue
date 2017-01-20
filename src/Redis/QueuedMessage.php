<?php
/**
 * @author: yevgen
 * @date: 20.01.17
 */

namespace YevhenHrytsai\JobQueue\Redis;


class QueuedMessage {
	/**
	 * @var QueueServer
	 */
	private $queue;
	/**
	 * @var int
	 */
	private $id;

	/**
	 * QueuedMessage constructor.
	 * @param QueueServer $queue
	 * @param int $id
	 */
	public function __construct(QueueServer $queue, $id)
	{
		$this->queue = $queue;
		$this->id = $id;
	}

	/**
	 * @return string
	 */
	public function getStatus()
	{
		$message = $this->queue->getMessageById($this->id);
		if ($message) {
			return $message['status'];
		}
		return 'deleted';
	}

	/**
	 * @return bool
	 */
	public function delete()
	{
		return $this->queue->deleteMessageById($this->id);
	}
}