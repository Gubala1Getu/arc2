<?php

/**
 * @author Benjamin Nowack <bnowack@semsol.com>
 * @author Konrad Abicht <konrad.abicht@pier-and-peer.com>
 * @license W3C Software License and GPL
 * @homepage <https://github.com/semsol/arc2>
 */

namespace ARC2\Store\Adapter;

/**
 * It provides an adapter instance for requested adapter name.
 */
class AdapterFactory
{
    /**
     * @param string $adapterName
     * @param array $configuration Default is array()
     */
    public function getInstanceFor($adapterName, $configuration = array())
    {
        if (in_array($adapterName, $this->getSupportedAdapters())) {
            if ('mysqli' == $adapterName) {
                return new mysqliAdapter($configuration);
            }
        }

        throw new \Exception(
            'Unknown adapter name given. Currently supported are: '
            .implode(', ', $this->getSupportedAdapters())
        );
    }

    /**
     * @return array
     */
    public function getSupportedAdapters()
    {
        return array('mysqli', 'pdo');
    }
}
