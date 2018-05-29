<?php

/**
 * @author Benjamin Nowack <bnowack@semsol.com>
 * @author Konrad Abicht <konrad.abicht@pier-and-peer.com>
 * @license W3C Software License and GPL
 * @homepage <https://github.com/semsol/arc2>
 */

namespace ARC2\Store\Adapter;

abstract class AbstractAdapter
{
    protected $configuration;
    protected $connection;

    /**
     * @param array $configuration Default is array(). Only use, if you have your own mysqli connection.
     */
    public function __construct(array $configuration = array())
    {
        $this->configuration = $configuration;

        $this->checkRequirements();
    }

    abstract public function checkRequirements();

    abstract public function connect($existingConnection = null);

    abstract public function close();

    abstract public function escapeVariable($value);

    abstract public function fetchAssoc($sql);

    abstract public function fetchRow($sql);

    abstract public function getConnectionId();

    abstract public function getDBSName();

    abstract public function getServerInfo();

    abstract public function getErrorCode();

    abstract public function getErrorMsg();

    abstract public function getNumberOfRows($sql);

    abstract public function query($sql, $resultmode = null);
}
