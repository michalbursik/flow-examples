<?php

declare(strict_types=1);

namespace FlowCustomizations;

use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunctionChain;
use Flow\ETL\Row;

final class Implode extends ScalarFunctionChain
{
    /**
     * @param non-empty-string $separator
     */
    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly string $separator,
    ) {
    }


    public function eval(Row $row): mixed
    {
        $val = $this->ref->eval($row);

        if (!\is_array($val)) {
            return $val;
        }

        return \implode($this->separator, $val);
    }
}