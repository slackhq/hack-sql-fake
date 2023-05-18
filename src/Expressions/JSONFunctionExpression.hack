namespace Slack\SQLFake;

use namespace HH\Lib\{C, Dict, Str, Vec};
use namespace Slack\SQLFake\JSONPath;

/**
 * emulates a call to a built-in MySQL JSON function
 * we implement as many as we want to in Hack
 */
final class JSONFunctionExpression extends BaseFunctionExpression {
	const ExpressionEvaluationOpts RETAIN_JSON_EVAL_OPTS = shape(
		'encode_json' => false,
	);

	const ExpressionEvaluationOpts RETAIN_ALL_EVAL_OPTS = shape(
		'encode_json' => false,
		'bool_as_int' => false,
	);

	const JSONPath\GetOptions UNWRAP_JSON_PATH_RESULTS = shape('unwrap' => true);

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
			case 'JSON_KEYS':
				return $this->sqlJSONKeys($row, $conn);
			case 'JSON_LENGTH':
				return $this->sqlJSONLength($row, $conn);
			case 'JSON_DEPTH':
				return $this->sqlJSONDepth($row, $conn);
			case 'JSON_CONTAINS':
				return $this->sqlJSONContains($row, $conn);
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

		$value = $args[0]->evaluate($row, $conn, self::RETAIN_JSON_EVAL_OPTS);
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

		$value = $args[0]->evaluate($row, $conn);
		if ($value is null) {
			return null;
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
	private function sqlJSONExtract(row $row, AsyncMysqlConnection $conn): ?WrappedJSON {
		$row = $this->maybeUnrollGroupedDataset($row);
		$args = $this->args;
		if (C\count($args) < 2) {
			throw new SQLFakeRuntimeException(
				'MySQL JSON_EXTRACT() function must be called with 1 JSON document & at least 1 JSON path',
			);
		}

		$json = $args[0]->evaluate($row, $conn);
		if ($json is null) {
			return null;
		}
		if (!($json is string)) {
			throw new SQLFakeRuntimeException('MySQL JSON_EXTRACT() function doc has incorrect type');
		}

		$jsonPathWithNulls = Vec\map(
			Vec\slice($args, 1),
			$a ==> {
				$evaled = $a->evaluate($row, $conn, self::RETAIN_JSON_EVAL_OPTS);
				if ($evaled is nonnull && !($evaled is string)) {
					throw new SQLFakeRuntimeException(
						'MySQL JSON_EXTRACT() function encountered non string JSON path argument',
					);
				}
				return $evaled;
			},
		);

		$jsonPaths = Vec\filter_nulls($jsonPathWithNulls);
		if (C\count($jsonPaths) !== C\count($jsonPathWithNulls)) {
			return null;
		}

		$results = vec[];
		try {
			$jsonObject = new JSONPath\JSONObject($json);

			if (C\count($jsonPaths) === 1) {
				// This is the only case, we can return the raw value instead of wrapping in vec[]
				$result = $jsonObject->get($jsonPaths[0], self::UNWRAP_JSON_PATH_RESULTS);
				if ($result is null) {
					return null;
				}
				return new WrappedJSON($result->value);
			}

			$results = C\reduce(
				$jsonPaths,
				($acc, $path) ==> {
					$result = $jsonObject->get($path, self::UNWRAP_JSON_PATH_RESULTS);
					if ($result is null) {
						return $acc;
					}
					$result = ($result->value is vec<_>) ? $result->value : vec[$result->value];
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

	private function sqlJSONReplace(row $row, AsyncMysqlConnection $conn): ?WrappedJSON {
		$row = $this->maybeUnrollGroupedDataset($row);
		$args = $this->args;
		$arg_count = C\count($args);
		if ($arg_count < 3 || $arg_count % 2 !== 1) {
			throw new SQLFakeRuntimeException(
				'MySQL JSON_REPLACE() function must be called with 1 JSON document & at least 1 JSON path + replacement value pair ',
			);
		}

		$json = $args[0]->evaluate($row, $conn);
		if ($json is null) {
			return null;
		}
		if (!($json is string)) {
			throw new SQLFakeRuntimeException('MySQL JSON_EXTRACT() function doc has incorrect type');
		}

		$replacementsWithNulls = Vec\slice($args, 1)
			|> Vec\map($$, $a ==> $a->evaluate($row, $conn, self::RETAIN_ALL_EVAL_OPTS))
			|> Vec\chunk($$, 2)
			|> Vec\map($$, $v ==> {
				$path = $v[0];
				if ($path is nonnull && !($path is string)) {
					throw new SQLFakeRuntimeException('MySQL JSON_REPLACE() function encountered non string JSON path');
				}

				$value = $v[1];
				if ($value is WrappedJSON) {
					$value = $value->rawValue();
				}
				return shape('path' => $path, 'value' => $value);
			});

		$replacements = vec[];
		// Doing it this way to please typechecker that replacements doesn't have any NULL path
		foreach ($replacementsWithNulls as $replacement) {
			$path = $replacement['path'];
			if ($path is nonnull) {
				$replacements[] = shape('path' => $path, 'value' => $replacement['value']);
			}
		}
		if (C\count($replacements) !== C\count($replacementsWithNulls)) {
			return null;
		}

		try {
			$current = new JSONPath\JSONObject($json);

			foreach ($replacements as $replacement) {
				$current = $current->replace($replacement['path'], $replacement['value'])->value;
			}

			return new WrappedJSON($current->getValue());
		} catch (JSONPath\JSONException $e) {
			throw new SQLFakeRuntimeException('MySQL JSON_REPLACE() function encountered error: '.$e->getMessage());
		}
	}

	private function sqlJSONKeys(row $row, AsyncMysqlConnection $conn): ?WrappedJSON {
		$row = $this->maybeUnrollGroupedDataset($row);
		$args = $this->args;

		$argCount = C\count($args);
		if ($argCount < 1) {
			throw new SQLFakeRuntimeException(
				'MySQL JSON_KEYS() function must be called with at least 1 JSON document & optionally 1 JSON path',
			);
		}

		$json = $args[0]->evaluate($row, $conn);
		if ($json is null) {
			return null;
		}
		if (!($json is string)) {
			throw new SQLFakeRuntimeException('MySQL JSON_KEYS() function doc has incorrect type');
		}

		$path = $argCount > 1 ? $args[1]->evaluate($row, $conn, self::RETAIN_JSON_EVAL_OPTS) : '$';
		if ($path is null) {
			return null;
		}
		if (!($path is string)) {
			throw new SQLFakeRuntimeException('MySQL JSON_KEYS() function path has incorrect type');
		}

		try {
			$keys = (new JSONPath\JSONObject($json))->keys($path);
			if ($keys is null) {
				return null;
			}
			return new WrappedJSON($keys->value);
		} catch (JSONPath\JSONException $e) {
			throw new SQLFakeRuntimeException('MySQL JSON_KEYS() function encountered error: '.$e->getMessage());
		}
	}

	private function sqlJSONLength(row $row, AsyncMysqlConnection $conn): ?int {
		$row = $this->maybeUnrollGroupedDataset($row);
		$args = $this->args;

		$argCount = C\count($args);
		if ($argCount < 1) {
			throw new SQLFakeRuntimeException(
				'MySQL JSON_LENGTH() function must be called with at least 1 JSON document & optionally 1 JSON path',
			);
		}

		$json = $args[0]->evaluate($row, $conn);
		if ($json is null) {
			return null;
		}
		if (!($json is string)) {
			throw new SQLFakeRuntimeException('MySQL JSON_LENGTH() function doc has incorrect type');
		}

		$path = $argCount > 1 ? $args[1]->evaluate($row, $conn, self::RETAIN_JSON_EVAL_OPTS) : '$';
		if ($path is null) {
			return null;
		}
		if (!($path is string)) {
			throw new SQLFakeRuntimeException('MySQL JSON_LENGTH() function path has incorrect type');
		}

		try {
			$keys = (new JSONPath\JSONObject($json))->length($path);
			if ($keys is null) {
				return null;
			}
			return $keys->value;
		} catch (JSONPath\JSONException $e) {
			throw new SQLFakeRuntimeException('MySQL JSON_LENGTH() function encountered error: '.$e->getMessage());
		}
	}

	private function sqlJSONDepth(row $row, AsyncMysqlConnection $conn): ?int {
		$row = $this->maybeUnrollGroupedDataset($row);
		$args = $this->args;

		$argCount = C\count($args);
		if ($argCount !== 1) {
			throw new SQLFakeRuntimeException(
				'MySQL JSON_DEPTH() function must be called with 1 JSON document argument',
			);
		}

		$json = $args[0]->evaluate($row, $conn);
		if ($json is null) {
			return null;
		}
		if (!($json is string)) {
			throw new SQLFakeRuntimeException('MySQL JSON_DEPTH() function doc has incorrect type');
		}

		try {
			return (new JSONPath\JSONObject($json))->depth()->value;
		} catch (JSONPath\JSONException $e) {
			throw new SQLFakeRuntimeException('MySQL JSON_DEPTH() function encountered error: '.$e->getMessage());
		}
	}

	private function sqlJSONContains(row $row, AsyncMysqlConnection $conn): ?bool {
		$row = $this->maybeUnrollGroupedDataset($row);
		$args = $this->args;
		$argCount = C\count($args);

		if ($argCount !== 2 && $argCount !== 3) {
			throw new SQLFakeRuntimeException('MySQL JSON_CONTAINS() function must be called with 2 - 3 arguments');
		}

		// Get the json from the column
		$json = $args[0]->evaluate($row, $conn);
		if ($json is null) {
			return null;
		}

		if (!($json is string)) {
			throw new SQLFakeRuntimeException('MySQL JSON_CONTAINS() function doc has incorrect type');
		}

		$path = '$'; // Set path to root initially in case no path is given
		if ($argCount === 3) {
			$path = $args[2]->evaluate($row, $conn, self::RETAIN_JSON_EVAL_OPTS);

			if (!($path is string)) {
				throw new SQLFakeRuntimeException('MySQL JSON_CONTAINS() function path has incorrect type');
			}
		}

		// Narrow down the json to the specified path
		try {
			$json = (new JSONPath\JSONObject($json))->get($path);
			if ($json is null || $json->value is null || !($json->value is vec<_>)) {
				throw new SQLFakeRuntimeException('MySQL JSON_CONTAINS() function given invalid json');
			}
			$json = $json->value[0];
		} catch (JSONPath\JSONException $e) {
			throw new SQLFakeRuntimeException('MySQL JSON_CONTAINS() function encountered error: '.$e->getMessage());
		}

		// Now check if the json contains the term
		try {
			$term = $args[1]->evaluate($row, $conn, self::RETAIN_JSON_EVAL_OPTS);

			if ($term is null) {
				return null;
			}

			if (!($term is string)) {
				throw new SQLFakeRuntimeException('MySQL JSON_CONTAINS() function value has incorrect type');
			}

			$term = (new JSONPath\JSONObject($term))->get('$');
			if ($term is null || $term->value is null || !($term->value is vec<_>)) {
				throw new SQLFakeRuntimeException('MySQL JSON_CONTAINS() function given invalid json');
			}
			$term = $term->value[0];

			if ($json is vec<_>) {
				// If $json is a vec then we have an array and will test if the array contains the given value
				if ($term is dict<_, _>) {
					return C\count(Vec\filter($json, $val ==> {
						if ($val is dict<_, _>) {
							return Dict\equal($val, $term);
						}
						return false;
					})) >
						0;
				} else {
					return C\contains($json, $term);
				}
			} else if ($json is dict<_, _>) {
				// If $json is a dict then we have an object and will test that either (1) $json and $term are the same or
				// (2) one of $json's members is the same as $term
				if ($term is dict<_, _>) {
					if (Dict\equal($json, $term)) {
						return true;
					}

					return C\count(Dict\filter($json, $val ==> {
						if ($val is dict<_, _>) {
							return Dict\equal($val, $term);
						}
						return false;
					})) >
						0;
				} else {
					return C\count(Dict\filter($json, $val ==> $term == $val)) > 0;
				}
			} else {
				return $json == $term;
			}

		} catch (JSONPath\JSONException $e) {
			throw new SQLFakeRuntimeException('MySQL JSON_CONTAINS() function encountered error: '.$e->getMessage());
		}

		return false;
	}
}
