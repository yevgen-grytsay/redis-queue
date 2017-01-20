<?php
/**
 * @author: yevgen
 * @date: 20.01.17
 */
use YevhenHrytsai\JobQueue\Redis\QueuedMessage;
use YevhenHrytsai\JobQueue\Redis\QueueServer;
use YevhenHrytsai\JobQueue\Redis\Sequence;

require_once __DIR__ . '/../../vendor/autoload.php';

$qname = 'players';
$server = new QueueServer(new Predis\Client(), new Sequence(new Predis\Client(), 'global_seq'), 'job_queue');
$queue = $server->queue($qname);
/** @var QueuedMessage[] $messages */
$messages = [
	$queue->enqueue('Suker'),
	$queue->enqueue('Baia'),
	$queue->enqueue('Stoichkov'),
	$queue->enqueue('Ince'),
];
var_dump(array_map(function (QueuedMessage $msg) {
	return $msg->getId();
}, $messages));
//$server->deleteMessageById($ids[0]);
$messages[0]->delete();

sleep(3);
while ($msg = $queue->pop()) {
	var_dump($msg);
}

//$client = new Predis\Client();
//$seq = new \YevhenHrytsai\JobQueue\Redis\Sequence($client, 'job_queue_pk_seq');
//var_dump($seq->nextValue());

function listOperations() {
	$client = new Predis\Client();
	$list = new \YevhenHrytsai\JobQueue\Redis\LinkedList($client, 'job_queue');
	$list->append(['Suker', 'Baia', 'Stoichkov', 'Ince']);
	$list->append(['Pepsi']);

	while ($el = $list->head()) {
		var_dump($el);
	}
}

function simpleAddAndRemove() {
	$client = new Predis\Client();
	$client->set('foo', 'bar');
	$value = $client->get('foo');
	var_dump($value);

	$client->del('foo');
	$value = $client->get('foo');
	var_dump($value);
}
