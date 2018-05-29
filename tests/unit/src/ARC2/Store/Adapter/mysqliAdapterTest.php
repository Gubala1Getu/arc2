<?php

namespace Tests\unit\src\ARC2\Store\Adapter;

use ARC2\Store\Adapter\mysqliAdapter;
use Tests\ARC2_TestCase;

class mysqliAdapterTest extends ARC2_TestCase
{
    public function setUp()
    {
        parent::setUp();

        // stop, if mysqli is not available
        if (false == \extension_loaded('mysqli') || false == \function_exists('mysqli_connect')) {
            $this->markTestSkipped('Test skipped, because extension mysqli is not installed.');
        }

        $this->fixture = new mysqliAdapter($this->dbConfig);
        $this->fixture->connect();

        // remove all tables
        $tables = $this->fixture->fetchAssoc('SHOW TABLES');
        if (is_array($tables)) {
            foreach($tables as $table) {
                $this->fixture->query('DROP TABLE '. $table[0]);
            }
        }
    }

    /*
     * Tests for connect
     */

    public function testConnectCreateNewConnection()
    {
        $this->fixture->close();

        // do explicit reconnect
        $this->fixture = new mysqliAdapter($this->dbConfig);
        $this->fixture->connect();

        $result = $this->fixture->query('SHOW TABLES');
        $this->assertTrue($result);
    }

    public function testConnectUseGivenConnection()
    {
        $this->fixture->close();

        // create connection outside of the instance
        $connection = mysqli_connect(
            $this->dbConfig['db_host'],
            $this->dbConfig['db_user'],
            $this->dbConfig['db_pwd'],
            $this->dbConfig['db_name']
        );

        $this->fixture = new mysqliAdapter();

        // use existing connection
        $this->fixture->connect($connection);

        // if not the same origin, the connection ID differs
        $this->assertEquals($this->fixture->getConnectionId(), mysqli_thread_id($connection));

        // simple test query to check that its working
        $result = $this->fixture->query('SHOW TABLES');
        $this->assertTrue($result);
    }

    /*
     * Tests for getDBSName
     */

    public function testGetDBSName()
    {
        $this->assertTrue(in_array($this->fixture->getDBSName(), array('mariadb', 'mysql')));
    }

    public function testGetDBSNameNoConnection()
    {
        $this->fixture->close();
        $this->assertNull($this->fixture->getDBSName());
    }

    /*
     * Tests for getErrorMsg and getErrorCode
     */

    public function testGetErrorMsgAndGetErrorCode()
    {
        // invalid query
        $result = $this->fixture->query('SHOW TABLES of x');
        $this->assertFalse($result);

        $dbs = 'mariadb' == $this->fixture->getDBSName() ? 'MariaDB' : 'MySQL';

        $this->assertEquals(
            "You have an error in your SQL syntax; check the manual that corresponds to your $dbs server version for the "
            ."right syntax to use near 'of x' at line 1",
            $this->fixture->getErrorMsg()
        );

        $this->assertEquals(1064, $this->fixture->getErrorCode());
    }

    /*
     * Tests for getNumberOfRows
     */

    public function testGetNumberOfRows()
    {
        // create test table
        $this->fixture->query('
            CREATE TABLE pet (name VARCHAR(20));
        ');

        $this->assertEquals(1, $this->fixture->getNumberOfRows('SHOW TABLES'));
    }

    public function testGetNumberOfRowsInvalidQuery()
    {
        // run with invalid query
        $this->assertEquals(0, $this->fixture->getNumberOfRows('SHOW TABLES of x'));
    }

    /*
     * Tests for query
     */

    public function testQuery()
    {
        // valid query
        $this->assertTrue($this->fixture->query('SHOW TABLES'));

        // invalid query
        $this->assertFalse($this->fixture->query('invalid query'));
    }
}
