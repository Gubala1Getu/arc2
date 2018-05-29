<?php

/**
 * Adapter to enable usage of mysqli_* functions.
 *
 * @author Benjamin Nowack <bnowack@semsol.com>
 * @author Konrad Abicht <konrad.abicht@pier-and-peer.com>
 * @license W3C Software License and GPL
 * @homepage <https://github.com/semsol/arc2>
 */

namespace ARC2\Store\Adapter;

class mysqliAdapter extends AbstractAdapter
{
    public function checkRequirements()
    {
        if (false == \extension_loaded('mysqli') || false == \function_exists('mysqli_connect')) {
            throw new \Exception('Extension mysqli is not loaded or fuciton mysqli_connect is not available.');
        }
    }

    /**
     * Connect to server or storing a given connection.
     */
    public function connect($existingConnection = null)
    {
        // reuse a given existing connection.
        // it assumes that $existingConnection is a mysqli connection object
        if (null !== $existingConnection) {
            $this->connection = $existingConnection;

        // create your own connection
        } elseif (null == $this->connection) {
            $this->connection = mysqli_connect(
                $this->configuration['db_host'],
                $this->configuration['db_user'],
                $this->configuration['db_pwd']
            );
        }

        // try to use given database. if it doesn't exist, try to create it
        if (isset($this->configuration['db_name'])
            && false === $this->query('USE `'.$this->configuration['db_name'].'`')) {
            $dbCreated = false;
            // try to create it
            if (isset($this->configuration['db_name']) && 0 < strlen($this->configuration['db_name'])) {
                $result = $this->query('
                    CREATE DATABASE IF NOT EXISTS `'.$this->configuration['db_name'].'`
                    DEFAULT CHARACTER SET utf8
                    DEFAULT COLLATE utf8_general_ci'
                );

                if ($result && $this->query('USE `'.$this->configuration['db_name'].'`')) {
                    $this->query("SET NAMES 'utf8'");
                    $dbCreated = true;
                }
            }
            // if db was not created, stop here and return string, which lead ARC2_Store to stop
            // and store given string was error message.
            if (false == $dbCreated) {
                return 'Database '.$this->configuration['db_name'].' not available. Creating it also failed.';
            }
        }

        // set names to UTF-8
        if (preg_match('/^utf8/', $this->getCollation())) {
            $this->query("SET NAMES 'utf8'");
        }

        // This is RDF, we may need many JOINs...
        // TODO find an equivalent in other DBS
        $this->query('SET SESSION SQL_BIG_SELECTS=1');

        return $this->connection;
    }

    public function close()
    {
        $result = mysqli_close($this->connection);
        $this->connection = null;
        return $result;
    }

    public function escapeVariable($value)
    {
        return mysqli_real_escape_string($this->connection, $value);
    }

    public function fetchAssoc($sql)
    {
        $result = mysqli_query($this->connection, $sql);

        $rows = array();

        if (false != $result) {
            while($row = $result->fetch_array()) {
                $rows[] = $row;
            }
        }

        return 0 < count($rows) ? $rows : false;
    }

    public function fetchRow($sql)
    {
        $result = mysqli_query($this->connection, $sql);

        if (false !== $result) {
            return $result->fetch_array();
        } else {
            return false;
        }
    }

    public function getCollation()
    {
        $row = $this->fetchRow('SHOW TABLE STATUS LIKE "'.$this->getTablePrefix().'setting"');

        if (isset($row['Collation'])) {
            return $row['Collation'];
        } else {
            return '';
        }
    }

    public function getConnectionId()
    {
        if (null != $this->connection) {
            return mysqli_thread_id($this->connection);
        }
    }

    public function getDBSName()
    {
        if (null == $this->connection) {
            return null;
        }

        return false !== strpos($this->connection->server_info, 'MariaDB')
            ? 'mariadb'
            : 'mysql';
    }

    public function getServerInfo()
    {
        return mysqli_get_server_info($this->connection);
    }

    public function getErrorCode()
    {
        return mysqli_errno($this->connection);
    }

    public function getErrorMsg()
    {
        return mysqli_error($this->connection);
    }

    public function getNumberOfRows($sql)
    {
        $result = mysqli_query($this->connection, $sql);
        if ($result) {
            return mysqli_num_rows($result);
        }
        return 0;
    }

    public function getStoreName()
    {
        if (isset($this->configuration['store_name'])) {
            return $this->configuration['store_name'];
        }

        return 'arc';
    }

    public function getTablePrefix()
    {
        $prefix = '';
        if (isset($this->configuration['db_table_prefix'])) {
            $prefix = $this->configuration['db_table_prefix'].'_';
        }

        $prefix .= $this->getStoreName().'_';
        return $prefix;
    }

    /**
     * @param string $sql Query
     *
     * @return bool True if query ran fine, false otherwise.
     */
    public function query($sql)
    {
        return mysqli_query($this->connection, $sql)
            ? true
            : false;
    }
}
