<?php

$primes_dir = '../primes/';

$m = new Mongo();
$db = $m->goldbach;
$db->primes->ensureIndex(array("prime" => 1));

foreach (glob($primes_dir."*.clean") as $filename) 
{
  $file = fopen($filename,'r');
  while(!feof($file)) { 
      $line = fgets($file);
      $primes = preg_split("/\s+/",trim($line));
      foreach ($primes as $prime)
        $db->primes->insert(array("prime" => (int) $prime));
  }
  fclose($file);
}

$db->primes->remove(array("prime" => 0));
