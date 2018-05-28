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
        $result = $this->fixture->queryDB('invalid-query', $this->dbConnection);
        $this->assertFalse($result);
    }
}
