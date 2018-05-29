<?php

require_once __DIR__ .'/../vendor/autoload.php';

error_reporting(E_ALL);

require 'ARC2_TestHandler.php';

global $dbConfig;

if (file_exists(__DIR__ .'/config.php')) {
    // use custom DB credentials, if available
    $dbConfig = require 'config.php';

} else {
    // standard DB credentials (ready to use in Travis)
    $dbConfig = array(
        'db_name' => 'testdb',
        'db_user' => 'root',
        'db_pwd'  => '',
        'db_host' => '127.0.0.1',
        'db_adapter' => 'mysqli'
    );
}
