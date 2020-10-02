namespace Slack\SQLFake;

use namespace HH\Lib\{C, Str};

/**
 * emulates a call to a built-in MySQL JSON function
 * we implement as many as we want to in Hack
 */
final class JSONFunctionExpression extends FunctionExpression {
    <<__Override>>
    public function evaluate(row $row, AsyncMysqlConnection $conn): mixed {
        switch ($this->functionName) {
            case 'JSON_VALID':
                return $this->sqlJSONValid($row, $conn);
            case 'JSON_EXTRACT':
                return $this->sqlJSONExtract($row, $conn);
        }

        throw new SQLFakeRuntimeException('Function '.$this->functionName.' not implemented yet');
    }

    private function sqlJSONValid(row $row, AsyncMysqlConnection $conn): int {
        $row = $this->maybeUnrollGroupedDataset($row);
        $args = $this->args;
        if (C\count($args) !== 1) {
            throw new SQLFakeRuntimeException('MySQL JSON_VALID() function must be called with one argument');
        }

        $value = Str\trim((string)$args[0]->evaluate($row, $conn));
        if ($value !== 'null' && \json_decode($value) is null) {
            return 0;
        }

        return 1;
    }

    private function sqlJSONExtract(row $row, AsyncMysqlConnection $conn): mixed {
        return null;
    }
}
