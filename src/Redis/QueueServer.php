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
    const STATUS_DISCARDED = 'discarded';
    /**
     * @var Client
     */
    private $client;
    /**
     * @var Sequence
     */
    private $sequence;
    /**
     * TODO can be refactored to static
     * @var \stdClass
     */
    private $terminator;

    /**
     * Queue constructor.
     * @param Client $client
     * @param Sequence $sequence
     */
    public function __construct(Client $client, Sequence $sequence)
    {
        $this->client = $client;
        $this->sequence = $sequence;
        $this->terminator = new \stdClass();
    }

    /**
     * @param $queueName
     * @param $payload
     * @return int
     */
    public function enqueue($queueName, $payload)
    {
        $id = $this->sequence->nextValue();
        $header = ['id' => $id, 'queue' => $queueName];
        $message = Delivery::encode(['id' => $id, 'payload' => $payload]);
        $this->client->transaction()
            ->hmset(static::headerKey($id), $header)
            ->lpush($queueName, [$message])
            ->exec();
        return $id;
    }

    /**
     * @param $id
     * @return string
     */
    public static function headerKey($id)
    {
        return 'headers:' . $id;
    }

    /**
     * @param $queue
     * @param $consumerId
     * @return string
     */
    public static function unackedKey($queue, $consumerId)
    {
        return $queue . ':unacked:' . $consumerId;
    }

    /**
     * Возвращает одно сообщение из личного пула неподтвержденных сообщений обратно в общую очередь.
     * Это может понадобиться, если воркер "упал", не успев подтвердить обработку сообщения.
     * Если в пуле нет сообщений, ничего не делает.
     *
     * Из соображений сделать API библиотеки как можно "тоньше",
     * запуск восстановления возлагается на клиентский код.
     *
     * @param $consumerId
     * @param $queue
     */
    public function recover($consumerId, $queue)
    {
        $backup = $this->client->rpop(self::unackedKey($queue, $consumerId));
        if (!$backup) return;
        $data = Delivery::decode($backup);
        $id = $data['id'];
        $this->client->transaction()
            ->hdel(self::headerKey($id), ['status'])
            ->rpush($queue, [$backup])
            ->exec();
    }

    /**
     * Берет одно сообщение из начала очереди.
     *
     * @param string $consumerId
     * @param string $queue
     * @param bool $blocking
     * @param int $timeoutSec
     * @return null|Delivery|\stdClass Метод возвращает
     *                                 1) null, если из очереди было взято сообщение, обработка которого была отменена
     *	                               2) экземпляр terminator, если в очереди нет сообщений;
     *                                 3) сообщение
     */
    public function pop($consumerId, $queue, $blocking = false, $timeoutSec = 10)
    {
        $unackedPool = static::unackedKey($queue, $consumerId);
        $message = null;
        $client = $this->client;
        $rawMessage = $blocking
            ? $client->brpoplpush($queue, $unackedPool, $timeoutSec)
            : $client->rpoplpush($queue, $unackedPool);
        $data = Delivery::decode($rawMessage);
        if (!$data) return $this->terminator;

        $id = $data['id'];
        /*
         * Предполагается, что единственная причина, по которой у сообщения, взятого из очереди,
         * есть статус, это установка статуса в DISCARDED с целью отменить обработку сообщения.
         */
        if ($client->hsetnx(static::headerKey($id), 'status', QueueServer::STATUS_PROCESSING)) {
            $message = new Delivery($data, static::headerKey($id), $unackedPool, $client);
        } else {
            $client->lpop($unackedPool);
        }

        return $message;
    }

    /**
     * @param $id
     * @return string
     */
    public function getMessageById($id)
    {
        return $this->client->get(static::headerKey($id));
    }

    /**
     * @param $id
     * @return bool
     */
    public function discardMessageById($id)
    {
        return $this->client->hsetnx(static::headerKey($id), 'status', self::STATUS_DISCARDED) === 1;
    }

    /**
     * @param $id
     * @return string
     */
    public function getStatusById($id)
    {
        return $this->client->hget(static::headerKey($id), 'status');
    }

    /**
     * @param $object
     * @return bool
     */
    public function isTerminator($object)
    {
        return $object === $this->terminator;
    }
}
