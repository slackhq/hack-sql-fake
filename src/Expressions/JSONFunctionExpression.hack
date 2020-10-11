namespace Slack\SQLFake;

use namespace HH\Lib\{C, Str, Vec};
use namespace Slack\SQLFake\JSONPath;

/**
 * emulates a call to a built-in MySQL JSON function
 * we implement as many as we want to in Hack
 */
final class JSONFunctionExpression extends FunctionExpression {
    const ExpressionEvaluationOpts RETAIN_ALL_EVAL_OPTS = shape(
        'unwrap_json' => false,
        'bool_as_int' => false,
    );

    <<__Override>>
    public function evaluateImpl(row $row, AsyncMysqlConnection $conn): mixed {
        switch ($this->functionName) {
            case 'JSON_VALID':
                return $this->sqlJSONValid($row, $conn);
            case 'JSON_QUOTE':
                return $this->sqlJSONQuote($row, $conn);
            case 'JSON_UNQUOTE':
                return $this->sqlJSONUnquote($row, $conn);
            case 'JSON_EXTRACT':
                return $this->sqlJSONExtract($row, $conn);
            case 'JSON_REPLACE':
                return $this->sqlJSONReplace($row, $conn);
        }

        throw new SQLFakeRuntimeException('Function '.$this->functionName.' not implemented yet');
    }

    private function sqlJSONValid(row $row, AsyncMysqlConnection $conn): ?bool {
        $row = $this->maybeUnrollGroupedDataset($row);
        $args = $this->args;
        if (C\count($args) !== 1) {
            throw new SQLFakeRuntimeException('MySQL JSON_VALID() function must be called with one argument');
        }

        $value = $args[0]->evaluate($row, $conn);
        if ($value is null) {
            return null;
        }

        if (!($value is string)) {
            return false;
        }

        $value = Str\trim($value);
        if ($value !== 'null' && \json_decode($value, true, 512, \JSON_FB_HACK_ARRAYS) is null) {
            return false;
        }

        return true;
    }

    private function sqlJSONQuote(row $row, AsyncMysqlConnection $conn): ?string {
        $row = $this->maybeUnrollGroupedDataset($row);
        $args = $this->args;
        if (C\count($args) !== 1) {
            throw new SQLFakeRuntimeException('MySQL JSON_QUOTE() function must be called with one argument');
        }

        $value = $args[0]->evaluate($row, $conn, self::RETAIN_ALL_EVAL_OPTS);
        if ($value is null) {
            return null;
        }

        if (!($value is string)) {
            throw new SQLFakeRuntimeException('MySQL JSON_QUOTE() function received invalid argument');
        }

        return \json_encode($value, \JSON_UNESCAPED_UNICODE);
    }

    private function sqlJSONUnquote(row $row, AsyncMysqlConnection $conn): ?string {
        $row = $this->maybeUnrollGroupedDataset($row);
        $args = $this->args;
        if (C\count($args) !== 1) {
            throw new SQLFakeRuntimeException('MySQL JSON_UNQUOTE() function must be called with one argument');
        }

        $value = $args[0]->evaluate($row, $conn, shape('unwrap_json' => false));
        if ($value is null) {
            return null;
        }

        if ($value is WrappedJSON) {
            $value = $value->asString();
        }

        if (!($value is string)) {
            throw new SQLFakeRuntimeException('MySQL JSON_UNQUOTE() function received non string argument');
        }

        // If it begins & ends with ", it must be a valid JSON string literal so use json_decode to validate
        // + decode
        if ($value is string && Str\starts_with($value, '"') && Str\ends_with($value, '"')) {
            $unquoted = \json_decode($value);
            if ($unquoted is null || !($unquoted is string)) {
                throw new SQLFakeRuntimeException('MySQL JSON_UNQUOTE() function received invalid argument');
            }
            return (string)$unquoted;
        }

        // MySQL doesn't seem to do anything at all if the string doesn't start & end with "
        return $value;
    }

    // Returns null, num or WrappedJSON
    private function sqlJSONExtract(row $row, AsyncMysqlConnection $conn): mixed {
        $row = $this->maybeUnrollGroupedDataset($row);
        $args = $this->args;
        if (C\count($args) < 2) {
            throw new SQLFakeRuntimeException(
                'MySQL JSON_EXTRACT() function must be called with 1 JSON document & at least 1 JSON path',
            );
        }

        $json = $args[0]->evaluate($row, $conn);
        if (!($json is string)) {
            throw new SQLFakeRuntimeException('MySQL JSON_EXTRACT() function doc has incorrect type');
        }

        $jsonPaths = Vec\map(
            Vec\slice($args, 1),
            $a ==> {
                $evaled = $a->evaluate($row, $conn, self::RETAIN_ALL_EVAL_OPTS);
                if (!($evaled is string)) {
                    throw new SQLFakeRuntimeException(
                        'MySQL JSON_EXTRACT() function encountered non string JSON path argument',
                    );
                }
                return $evaled;
            },
        );

        $results = vec[];
        try {
            $jsonObject = new JSONPath\JSONObject($json, true);

            if (C\count($jsonPaths) === 1) {
                // This is the only case, we can return the raw value instead of wrapping in vec[]
                $result = $jsonObject->get($jsonPaths[0]);
                return new WrappedJSON($result);
            }

            $results = C\reduce(
                $jsonPaths,
                ($acc, $path) ==> {
                    $result = $jsonObject->get($path);
                    if ($result is null) {
                        return $acc;
                    }
                    $result = ($result is vec<_>) ? $result : vec[$result];
                    return Vec\concat($acc, $result);
                },
                vec[],
            );
        } catch (JSONPath\JSONException $e) {
            throw new SQLFakeRuntimeException('MySQL JSON_EXTRACT() function encountered error: '.$e->getMessage());
        }

        if (C\is_empty($results)) {
            return null;
        }

        return new WrappedJSON($results);
    }

    private function sqlJSONReplace(row $row, AsyncMysqlConnection $conn): mixed {
        $row = $this->maybeUnrollGroupedDataset($row);
        $args = $this->args;
        if (C\count($args) % 2 !== 1) {
            throw new SQLFakeRuntimeException(
                'MySQL JSON_REPLACE() function must be called with 1 JSON document & at least 1 JSON path + replacement value pair ',
            );
        }

        $json = $args[0]->evaluate($row, $conn);
        if (!($json is string)) {
            throw new SQLFakeRuntimeException('MySQL JSON_EXTRACT() function doc has incorrect type');
        }

        $jsonPathValuePairs = Vec\slice($args, 1)
            |> Vec\map($$, $a ==> $a->evaluate($row, $conn, self::RETAIN_ALL_EVAL_OPTS))
            |> Vec\chunk($$, 2)
            |> Vec\map($$, $v ==> {
                $path = $v[0];
                if (!($path is string)) {
                    throw new SQLFakeRuntimeException('MySQL JSON_REPLACE() function encountered non string JSON path');
                }

                $value = $v[1];
                if ($value is WrappedJSON) {
                    $value = $value->rawValue();
                }
                return shape('path' => $path, 'value' => $value);
            });

        try {
            $current = new JSONPath\JSONObject($json);

            foreach ($jsonPathValuePairs as $replacement) {
                $current = $current->replace($replacement['path'], $replacement['value']);
            }

            return new WrappedJSON($current->getValue());
        } catch (JSONPath\JSONException $e) {
            throw new SQLFakeRuntimeException('MySQL JSON_REPLACE() function encountered error: '.$e->getMessage());
        }
    }
}
