<?php

namespace Tests\integration\store;

/**
 * Run ARC2_Store tests using mysqli as adapter backend.
 */
class mysqliARC2_StoreTest extends AbstractARC2_StoreTest
{
    public function setUp()
    {
        parent::setUp();

        // set adapter explicit
        $this->dbConfig['db_adapter'] = 'mysqli';

        $this->fixture = \ARC2::getStore($this->dbConfig);
        $this->fixture->createDBCon();

        // remove all tables
        $tables = $this->fixture->getDBAdapter()->fetchAssoc('SHOW TABLES');
        if (is_array($tables)) {
            foreach($tables as $table) {
                $this->fixture->getDBAdapter()->query('DROP TABLE '. $table[0]);
            }
        }

        // fresh setup of ARC2
        $this->fixture->setup();
    }

    public function tearDown()
    {
        $this->fixture->closeDBCon();
    }
}
