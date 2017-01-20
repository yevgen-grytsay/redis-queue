<?php
/**
 * @author: yevgen
 * @date: 20.01.17
 */
use YevhenHrytsai\JobQueue\Redis\QueueServer;
use YevhenHrytsai\JobQueue\Redis\Sequence;

require_once __DIR__ . '/../../vendor/autoload.php';

$qname = 'players';
$server = new QueueServer(new Predis\Client(), new Sequence(new Predis\Client(), 'global_seq'), 'job_queue');
$server->enqueue($qname, 'Suker');
$server->enqueue($qname, 'Baia');
$server->enqueue($qname, 'Stoichkov');
$server->enqueue($qname, 'Ince');

while ($msg = $server->pop($qname)) {
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
