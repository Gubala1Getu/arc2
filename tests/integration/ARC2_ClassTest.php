<?php

namespace Tests\integration;

use Tests\ARC2_TestCase;

class ARC2_ClassTest extends ARC2_TestCase
{
    protected $dbConnection;
    protected $store;

    public function setUp()
    {
        parent::setUp();

        $this->store = \ARC2::getStore($this->dbConfig);
        $this->store->setup();
        $this->dbConnection = $this->store->getDBCon();

        $this->fixture = new \ARC2_Class([], $this);
    }

    /*
     * Tests for queryDB
     */

    public function testQueryDB()
    {
        $result = $this->fixture->queryDB('SHOW TABLES', $this->dbConnection);
        $this->assertEquals(1, $result->field_count);
        $this->assertEquals(6, $result->num_rows);
    }

    public function testQueryDBInvalidQuery()
    {
        $result = $this->fixture->queryDB('SHOW TABLES of x', $this->dbConnection);
        $this->assertFalse($result);

        $dbs = 'mariadb' == $this->store->getDBSName() ? 'MariaDB' : 'MySQL';

        $this->assertEquals(
            "You have an error in your SQL syntax; check the manual that corresponds to your $dbs server version for the "
            ."right syntax to use near 'of x' at line 1",
            $this->store->getDBAdapter()->getErrorMsg()
        );
    }
}
