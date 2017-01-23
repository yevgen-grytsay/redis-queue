<?php
/**
 * @author: yevgen
 * @date: 21.01.17
 */

namespace YevhenHrytsai\JobQueue\Redis;


use Predis\Client;

class Delivery {
    /**
     * @var array
     */
    private $data;
    /**
     * @var string
     */
    private $key;
    /**
     * @var string
     */
    private $unackedPool;
    /**
     * @var Client
     */
    private $client;

    /**
     * Delivery constructor.
     * @param array $data
     * @param string $key
     * @param string $unackedPool
     * @param Client $client
     */
    public function __construct(array $data, $key, $unackedPool, Client $client)
    {
        $this->data = $data;
        $this->key = $key;
        $this->unackedPool = $unackedPool;
        $this->client = $client;
    }

    /**
     * @param array $message
     * @return string
     */
    public static function encode(array $message)
    {
        return json_encode($message);
    }

    /**
     * @param $rawMessage
     * @return mixed
     */
    public static function decode($rawMessage)
    {
        return json_decode($rawMessage, true);
    }

    public function ack()
    {
        $this->client->transaction()
            ->hset($this->key, 'status', QueueServer::STATUS_ACKNOWLEDGED)
            ->rpop($this->unackedPool) //TODO use lrem?
            ->exec();
    }

    /**
     * @return string
     */
    public function getPayload()
    {
        return $this->data['payload'];
    }

    /**
     * @return string
     */
    public function getId()
    {
        return $this->data['id'];
    }
}