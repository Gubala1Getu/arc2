<?php

/**
 * ARC2 RDF Store.
 *
 * @author Benjamin Nowack <bnowack@semsol.com>
 * @author Konrad Abicht <konrad.abicht@pier-and-peer.com>
 * @license W3C Software License and GPL
 * @homepage <https://github.com/semsol/arc2>
 */

namespace ARC2\Store\Adapter;

class mysqliAdapter
{
    protected $configuration;
    protected $connection;

    /**
     * @param array $configuration Default is array(). Only use, if you have your own mysqli connection.
     */
    public function __construct(array $configuration = array())
    {
        $this->configuration = $configuration;
    }

    public function connect($existingConnection = null)
    {
        // reuse a given existing connection
        if (null !== $existingConnection) {
            $this->connection = $existingConnection;

        // create your own connection
        } elseif (null == $this->connection) {
            $this->connection = mysqli_connect(
                $this->configuration['db_host'],
                $this->configuration['db_user'],
                $this->configuration['db_pwd'],
                $this->configuration['db_name']
            );
        }
        return $this->connection;
    }

    public function close()
    {
        return mysqli_close($this->connection);
    }

    public function escapeVariable($value)
    {
        return mysqli_real_escape_string($this->connection, $value);
    }

    public function fetchAssoc($sql)
    {
        $result = mysqli_query($this->connection, $sql);

        $rows = array();

        while($row = $result->fetch_array()) {
            $rows[] = $row;
        }

        return 0 < count($rows) ? $rows : false;
    }

    public function fetchRow($sql)
    {
        $result = mysqli_query($this->connection, $sql);

        return $result->fetch_array();
    }

    public function getConnectionId()
    {
        if (null != $this->connection) {
            return mysqli_thread_id($this->connection);
        }
    }

    public function getServerInfo()
    {
        return mysqli_get_server_info($this->connection);
    }

    public function getErrorCode()
    {
        return mysqli_connect_errno($this->connection);
    }

    public function getErrorMsg()
    {
        return mysqli_connect_error($this->connection);
    }

    public function getNumberOfRows($sql)
    {
        $result = $this->query($sql);
        if ($result) {
            return mysqli_num_rows($result);
        }
        return 0;
    }

    public function query($sql, $resultmode = \MYSQLI_STORE_RESULT)
    {
        // if there is no connection, try to connect to server.
        if (null == $this->connection) {
            $this->connect();
        }

        return mysqli_query($this->connection, $sql, $resultmode);
    }
}
