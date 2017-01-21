<?php
/**
 * @author: yevgen
 * @date: 20.01.17
 */
use YevhenHrytsai\JobQueue\Redis\QueueServer;
use YevhenHrytsai\JobQueue\Redis\Sequence;

require_once __DIR__ . '/../../vendor/autoload.php';

//class LPopRPush extends Predis\Command\ScriptCommand
//{
//	public function getKeysCount()
//	{
//		return 2;
//	}
//
//	public function getScript()
//	{
//		return <<<LUA
//local el = redis.call('LPOP', KEYS[1])
//if el ~= nil then
//	return redis.call('RPUSH', KEYS[2], el)
//end
//return nil
//LUA;
//	}
//}

$qname = 'players';
$client = new Predis\Client(null, ['prefix' => 'job_queue:']);
//$client->getProfile()->defineCommand('lpoprpush', 'LPopRPush');
$server = new QueueServer($client, new Sequence($client, 'global_seq'));
$ids = [
	$server->enqueue($qname, 'Suker'),
	$server->enqueue($qname, 'Baia'),
	$server->enqueue($qname, 'Stoichkov'),
	$server->enqueue($qname, 'Ince'),
	$server->enqueue($qname, 'Pepsi'),
];
var_dump($ids);
//TODO sleep when there are no messages
foreach ($server->consumer(1, $qname)->consume() as $item) {
	var_dump($item);
//	throw new RuntimeException("Faulty consumer");
}

//$client = new Predis\Client();
//$seq = new \YevhenHrytsai\JobQueue\Redis\Sequence($client, 'job_queue_pk_seq');
//var_dump($seq->nextValue());

function simpleAddAndRemove() {
	$client = new Predis\Client();
	$client->set('foo', 'bar');
	$value = $client->get('foo');
	var_dump($value);

	$client->del('foo');
	$value = $client->get('foo');
	var_dump($value);
}
