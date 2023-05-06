namespace Slack\SQLFake;

use function Facebook\FBExpect\expect;
use type Facebook\HackTest\{DataProvider, HackTest};

type JSONValidExpectedType = shape(?'exception' => classname<SQLFakeException>, ?'value' => ?int);
type JSONQuoteExpectedType = shape(?'exception' => classname<SQLFakeException>, ?'value' => ?string);
type JSONUnquoteExpectedType = shape(?'exception' => classname<SQLFakeException>, ?'value' => ?string);
type JSONExtractExpectedType = shape(?'exception' => classname<SQLFakeException>, ?'value' => ?string);
type JSONReplaceExpectedType = shape(?'exception' => classname<SQLFakeException>, ?'value' => ?string);
type JSONKeysExpectedType = shape(?'exception' => classname<SQLFakeException>, ?'value' => ?string);
type JSONLengthExpectedType = shape(?'exception' => classname<SQLFakeException>, ?'value' => ?int);
type JSONDepthExpectedType = shape(?'exception' => classname<SQLFakeException>, ?'value' => ?int);
type JSONFunctionCompositionExpectedType = shape(?'exception' => classname<SQLFakeException>, ?'value' => mixed);
type JSONContainsExpectedType = shape(?'exception' => classname<SQLFakeException>, ?'value' => ?int);

final class JSONFunctionTest extends HackTest {
	private static ?AsyncMysqlConnection $conn;

	<<__Override>>
	public static async function beforeFirstTestAsync(): Awaitable<void> {
		static::$conn = await SharedSetup::initAsync();
		// block hole logging
		Logger::setHandle(new \HH\Lib\IO\MemoryHandle());
	}

	<<__Override>>
	public async function beforeEachTestAsync(): Awaitable<void> {
		restore('setup');
		QueryContext::$strictSchemaMode = false;
		QueryContext::$strictSQLMode = false;
	}

	public static async function testJSONValidProvider(): Awaitable<vec<(string, JSONValidExpectedType)>> {
		return vec[
			// Invalid input
			tuple('JSON_VALID()', shape('exception' => SQLFakeRuntimeException::class)),
			tuple('JSON_VALID(1, 2)', shape('exception' => SQLFakeRuntimeException::class)),

			// NULL input
			tuple('JSON_VALID(NULL)', shape('value' => null)),

			// Valid
			tuple("JSON_VALID('null')", shape('value' => 1)),
			tuple("JSON_VALID('{\"key\": \"value\"}')", shape('value' => 1)),
			tuple("JSON_VALID('[{\"key\": \"value\"}]')", shape('value' => 1)),
			tuple("JSON_VALID('\"\"')", shape('value' => 1)),
			tuple("JSON_VALID('true')", shape('value' => 1)),
			tuple("JSON_VALID('false')", shape('value' => 1)),
			tuple("JSON_VALID('2')", shape('value' => 1)),

			// Invalid
			tuple("JSON_VALID(' ')", shape('value' => 0)),
			tuple("JSON_VALID('arbitrary_string')", shape('value' => 0)),
			tuple('JSON_VALID(2)', shape('value' => 0)),
			tuple('JSON_VALID(TRUE)', shape('value' => 0)),
		];
	}

	<<DataProvider('testJSONValidProvider')>>
	public async function testJSONValid(string $select, JSONValidExpectedType $expected): Awaitable<void> {
		await $this->simpleSelectTestCase($select, $expected);
	}

	public static async function testJSONQuoteProvider(): Awaitable<vec<(string, JSONQuoteExpectedType)>> {
		return vec[
			tuple('JSON_QUOTE(NULL)', shape('value' => null)),
			tuple("JSON_QUOTE('null')", shape('value' => '"null"')),
			tuple("JSON_QUOTE('a')", shape('value' => '"a"')),
			tuple("JSON_QUOTE('{\"a\":\"c\"}')", shape('value' => '"{\"a\":\"c\"}"')),
			tuple("JSON_QUOTE('[1, \"\"\"]')", shape('value' => '"[1, \"\"\"]"')),
			tuple(
				"JSON_QUOTE('[1, \"\\\\\"]')",
				shape('value' => '"[1, \"\\\\\"]"'),
			), // In PHP \\ represents \ in single & double quoted strings
			tuple("JSON_QUOTE('►')", shape('value' => '"►"')), // MySQL doesn't seem to escape these
			tuple("JSON_QUOTE('2\n2')", shape('value' => '"2\\n2"')), // Escapes newline
			tuple("JSON_QUOTE('".'22'.\chr(8)."')", shape('value' => '"22\\b"')), // Escapes backspace character

			// invalid
			tuple('JSON_QUOTE(TRUE)', shape('exception' => SQLFakeRuntimeException::class)),
			tuple('JSON_QUOTE(45)', shape('exception' => SQLFakeRuntimeException::class)),
		];
	}

	<<DataProvider('testJSONQuoteProvider')>>
	public async function testJSONQuote(string $select, JSONQuoteExpectedType $expected): Awaitable<void> {
		await $this->simpleSelectTestCase($select, $expected);
	}

	public static async function testJSONUnquoteProvider(): Awaitable<vec<(string, JSONUnquoteExpectedType)>> {
		return vec[
			tuple('JSON_UNQUOTE(NULL)', shape('value' => null)),
			tuple('JSON_UNQUOTE("null")', shape('value' => 'null')),
			tuple('JSON_UNQUOTE("a")', shape('value' => 'a')),
			tuple("JSON_UNQUOTE('\\\\')", shape('value' => '\\')), // no-op
			tuple('JSON_UNQUOTE(\'"{\\\\\"a\\\\\":\\\\\"c\\\\\"}"\')', shape('value' => '{"a":"c"}')),
			tuple('JSON_UNQUOTE(\'[1, """]\')', shape('value' => '[1, """]')), // no-op
			tuple('JSON_UNQUOTE(\'"[1, \\\"\\\"\\\"]"\')', shape('value' => '[1, """]')),
			tuple('JSON_UNQUOTE(\'"\\\\\\\\"\')', shape('value' => '\\')),
			tuple('JSON_UNQUOTE(\'"\\\u25ba"\')', shape('value' => '►')),
			tuple('JSON_UNQUOTE(\'"\\\\n"\')', shape('value' => "\n")),
			tuple('JSON_UNQUOTE(\'"2\\\\b"\')', shape('value' => '2'.\chr(8))),

			// invalid
			tuple(
				'JSON_UNQUOTE(\'"\\\\"\')',
				shape('exception' => SQLFakeRuntimeException::class),
			), // inner function receive '"\"'
			tuple('JSON_UNQUOTE(2)', shape('exception' => SQLFakeRuntimeException::class)),
		];
	}

	<<DataProvider('testJSONUnquoteProvider')>>
	public async function testJSONUnquote(string $select, JSONUnquoteExpectedType $expected): Awaitable<void> {
		await $this->simpleSelectTestCase($select, $expected);
	}

	public static async function testJSONExtractProvider(): Awaitable<vec<(string, JSONExtractExpectedType)>> {
		return vec[
			// invalid input
			tuple('JSON_EXTRACT()', shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_EXTRACT('[]')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_EXTRACT('[}', '$.a')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_EXTRACT('[]', 'a.b')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_EXTRACT('[]', 2)", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_EXTRACT('[]', TRUE)", shape('exception' => SQLFakeRuntimeException::class)),

			// NULL input
			tuple("JSON_EXTRACT(NULL, '$.a')", shape('value' => null)),
			tuple("JSON_EXTRACT('{\"a\": {\"b\": \"test\"}}', NULL)", shape('value' => null)),

			// valid
			tuple("JSON_EXTRACT('{\"a\": {\"b\": \"test\"}}', '$.a.b')", shape('value' => '"test"')),
			tuple("JSON_EXTRACT('{\"a\": {\"b\": 2}}', '$.a.b')", shape('value' => '2')),
			tuple("JSON_EXTRACT('{\"a\": {\"b\": true}}', '$.a.b')", shape('value' => 'true')),
			tuple(
				"JSON_EXTRACT('{\"a\": {\"b\": 2, \"c\": \"test\"}}', '$.a.b', '$.\"a\".\"c\"')",
				shape('value' => '[2,"test"]'),
			),
			tuple("JSON_EXTRACT('[{\"a\": [{\"b\": \"test\"}]}]', '$[0].a')", shape('value' => '[{"b":"test"}]')),
			tuple("JSON_EXTRACT('[{\"a\": [{\"b\": \"test\"}]}]', '$[0].a[0]')", shape('value' => '{"b":"test"}')),
			tuple("JSON_EXTRACT('[{\"a\": [{\"b\": \"test\"}]}]', '$**.\"b\"')", shape('value' => '["test"]')),
			tuple(
				"JSON_EXTRACT('[{\"a\": [{\"b\": \"test\", \"c\":false}]}]', '$**.*')",
				shape('value' => '[[{"b":"test","c":false}],"test",false]'),
			),
			tuple("JSON_EXTRACT('{\"a\": {\"b\": [\"test\",10]}}', '$**[1]')", shape('value' => '[10]')),
			tuple("JSON_EXTRACT('{\"a\":2}', '$.a', '$.b')", shape('value' => '[2]')),
			tuple("JSON_EXTRACT('\"a\"', '$')", shape('value' => '"a"')),

			// non-existent
			tuple("JSON_EXTRACT('2', '$**[1]')", shape('value' => null)),
			tuple("JSON_EXTRACT('{\"a\": 2}', '$.b')", shape('value' => null)),
			tuple("JSON_EXTRACT('{\"a\": 2}', '$.b', '$[0]')", shape('value' => null)),
		];
	}

	<<DataProvider('testJSONExtractProvider')>>
	public async function testJSONExtract(string $select, JSONExtractExpectedType $expected): Awaitable<void> {
		await $this->simpleSelectTestCase($select, $expected);
	}

	public static async function testJSONReplaceProvider(): Awaitable<vec<(string, JSONReplaceExpectedType)>> {
		return vec[
			// NULL inputs
			tuple("JSON_REPLACE(NULL, '$.a', 2)", shape('value' => null)),
			tuple("JSON_REPLACE('{}', NULL, 2)", shape('value' => null)),

			// bad input
			tuple("JSON_REPLACE('{}')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_REPLACE('', '$', 2)", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_REPLACE('{}', '$', 2, '$')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_REPLACE('{}', '\$fd', 2)", shape('exception' => SQLFakeRuntimeException::class)),

			// non-existent path (no-op)
			tuple("JSON_REPLACE('null', '$.a', 2)", shape('value' => 'null')),
			tuple("JSON_REPLACE('2', '$.a', 45)", shape('value' => '2')),
			tuple("JSON_REPLACE('{\"a\": {\"b\":\"test\"}}', '$.b', 45)", shape('value' => '{"a":{"b":"test"}}')),

			// existent path
			tuple("JSON_REPLACE('{}', '$', TRUE)", shape('value' => 'true')),
			tuple("JSON_REPLACE('[1]', '$[0]', NULL)", shape('value' => '[null]')),
			tuple("JSON_REPLACE('{\"a\":{\"b\":\"test\"}}', '$.a.b', 2)", shape('value' => '{"a":{"b":2}}')),

			// multiple
			tuple("JSON_REPLACE('{\"a\": [1,2]}', '$.a[0]', 3, '$.a[1]', 4)", shape('value' => '{"a":[3,4]}')),

			// successive replacements work off interim object
			tuple("JSON_REPLACE('{\"a\": [1,2]}', '$.a[0]', 3, '$.a[0]', 4)", shape('value' => '{"a":[4,2]}')),
		];
	}

	<<DataProvider('testJSONReplaceProvider')>>
	public async function testJSONReplace(string $select, JSONReplaceExpectedType $expected): Awaitable<void> {
		await $this->simpleSelectTestCase($select, $expected);
	}

	public static async function testJSONKeysProvider(): Awaitable<vec<(string, JSONKeysExpectedType)>> {
		return vec[
			// invalid input
			tuple("JSON_KEYS('{]')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple('JSON_KEYS(2)', shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_KEYS('{}', 2)", shape('exception' => SQLFakeRuntimeException::class)),

			// null inputs
			tuple('JSON_KEYS(NULL)', shape('value' => null)),
			tuple("JSON_KEYS('{}', NULL)", shape('value' => null)),

			// points to non-object
			tuple("JSON_KEYS('[]')", shape('value' => null)),
			tuple("JSON_KEYS('2')", shape('value' => null)),
			tuple("JSON_KEYS('true')", shape('value' => null)),
			tuple("JSON_KEYS('null')", shape('value' => null)),
			tuple("JSON_KEYS('{\"a\":2}', '$.a')", shape('value' => null)),
			tuple("JSON_KEYS('[2]', '$[0]')", shape('value' => null)),

			// path is divergent
			tuple("JSON_KEYS('{}', '$[*]')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_KEYS('{}', '$**.a')", shape('exception' => SQLFakeRuntimeException::class)),

			// points to object
			tuple("JSON_KEYS('{}')", shape('value' => '[]')),
			tuple("JSON_KEYS('{\"a\": {\"b\": 2, \"c\": true}}')", shape('value' => '["a"]')),
			tuple("JSON_KEYS('{\"a\": {\"b\": 2, \"c\": true}}', '$.a')", shape('value' => '["b","c"]')),
			tuple("JSON_KEYS('[{\"a\": {\"b\": 2, \"c\": true}}]', '$[0]')", shape('value' => '["a"]')),
			tuple("JSON_KEYS('[{\"a\": {\"b\": 2, \"c\": true}}]', '$[0].a')", shape('value' => '["b","c"]')),
		];
	}

	<<DataProvider('testJSONKeysProvider')>>
	public async function testJSONKeys(string $select, JSONKeysExpectedType $expected): Awaitable<void> {
		await $this->simpleSelectTestCase($select, $expected);
	}

	public static async function testJSONLengthProvider(): Awaitable<vec<(string, JSONLengthExpectedType)>> {
		return vec[
			// invalid input
			tuple("JSON_LENGTH('{]')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple('JSON_LENGTH(2)', shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_LENGTH('{}', 2)", shape('exception' => SQLFakeRuntimeException::class)),

			// null inputs
			tuple('JSON_LENGTH(NULL)', shape('value' => null)),
			tuple("JSON_LENGTH('{}', NULL)", shape('value' => null)),

			// path is divergent
			tuple("JSON_LENGTH('{}', '$[*]')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_LENGTH('{}', '$**.a')", shape('exception' => SQLFakeRuntimeException::class)),

			// points to object
			tuple("JSON_LENGTH('{}')", shape('value' => 0)),
			tuple("JSON_LENGTH('{\"a\": {\"b\": 2, \"c\": true}}')", shape('value' => 1)),
			tuple("JSON_LENGTH('{\"a\": {\"b\": 2, \"c\": true}}', '$.a')", shape('value' => 2)),
			tuple("JSON_LENGTH('[{\"a\": {\"b\": 2, \"c\": true}}]', '$[0]')", shape('value' => 1)),
			tuple("JSON_LENGTH('[{\"a\": {\"b\": 2, \"c\": true}}]', '$[0].a')", shape('value' => 2)),

			// points to vec
			tuple("JSON_LENGTH('[]')", shape('value' => 0)),
			tuple("JSON_LENGTH('[{\"a\": 2}]')", shape('value' => 1)),
			tuple("JSON_LENGTH('{\"a\": [true, 1]}', '$.a')", shape('value' => 2)),
			tuple("JSON_LENGTH('[[1], 3]', '$[0]')", shape('value' => 1)),
			tuple("JSON_LENGTH('[{\"a\": [{\"b\": true}, false, 4]}]', '$[0].a')", shape('value' => 3)),

			// points to scalar
			tuple("JSON_LENGTH('\"string\"')", shape('value' => 1)),
			tuple("JSON_LENGTH('1')", shape('value' => 1)),
			tuple("JSON_LENGTH('true')", shape('value' => 1)),
			tuple("JSON_LENGTH('null')", shape('value' => 1)),
			tuple("JSON_LENGTH('{\"a\": \"string\", \"b\": 2}', '$.a')", shape('value' => 1)),
			tuple("JSON_LENGTH('{\"a\": \"string\", \"b\": 2}', '$.b')", shape('value' => 1)),
			tuple("JSON_LENGTH('{\"a\": \"string\", \"b\": true}', '$.b')", shape('value' => 1)),
			tuple("JSON_LENGTH('{\"a\": \"string\", \"b\": null}', '$.b')", shape('value' => 1)),
		];
	}

	<<DataProvider('testJSONLengthProvider')>>
	public async function testJSONLength(string $select, JSONLengthExpectedType $expected): Awaitable<void> {
		await $this->simpleSelectTestCase($select, $expected);
	}

	public static async function testJSONDepthProvider(): Awaitable<vec<(string, JSONDepthExpectedType)>> {
		return vec[
			// invalid input
			tuple('JSON_DEPTH()', shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_DEPTH('{]')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple('JSON_DEPTH(2)', shape('exception' => SQLFakeRuntimeException::class)),
			tuple('JSON_DEPTH(TRUE)', shape('exception' => SQLFakeRuntimeException::class)),

			// null input
			tuple('JSON_DEPTH(NULL)', shape('value' => null)),

			// depth 1
			tuple("JSON_DEPTH('2')", shape('value' => 1)),
			tuple("JSON_DEPTH('true')", shape('value' => 1)),
			tuple("JSON_DEPTH('false')", shape('value' => 1)),
			tuple("JSON_DEPTH('null')", shape('value' => 1)),
			tuple("JSON_DEPTH('\"string\"')", shape('value' => 1)),
			tuple("JSON_DEPTH('{}')", shape('value' => 1)),
			tuple("JSON_DEPTH('[]')", shape('value' => 1)),

			// depth 2
			tuple(
				"JSON_DEPTH('{\"a\": 2, \"b\": [], \"c\": \"string\", \"d\": null, \"e\": true, \"f\": {}}')",
				shape('value' => 2),
			),
			tuple("JSON_DEPTH('[2, {}, [], \"string\", null, false]')", shape('value' => 2)),

			// depth > 2
			tuple("JSON_DEPTH('[[2]]')", shape('value' => 3)),
			tuple("JSON_DEPTH('{\"a\": {\"b\": []}}')", shape('value' => 3)),
			tuple("JSON_DEPTH('{\"a\": {\"b\": [true]}}')", shape('value' => 4)),
			tuple("JSON_DEPTH('[2, 1, [{\"a\": [3, 4]}]]')", shape('value' => 5)),
			tuple("JSON_DEPTH('[2, 1, [{\"a\": [3, [{\"a\": [2]}]]}]]')", shape('value' => 8)),
		];
	}

	<<DataProvider('testJSONDepthProvider')>>
	public async function testJSONDepth(string $select, JSONDepthExpectedType $expected): Awaitable<void> {
		await $this->simpleSelectTestCase($select, $expected);
	}

	public static async function testJSONFunctionCompositionProvider(
	): Awaitable<vec<(string, JSONFunctionCompositionExpectedType)>> {
		return vec[
			// JSON as doc for JSON_EXTRACT
			tuple("JSON_EXTRACT(JSON_EXTRACT('{\"a\":true}', '$'), '$.a')", shape('value' => 'true')),
			tuple("JSON_EXTRACT(JSON_EXTRACT('[true]', '$[0]'), '$')", shape('value' => 'true')),

			// JSON as doc for JSON_REPLACE
			tuple(
				"JSON_REPLACE(JSON_EXTRACT('{\"a\":{\"b\": 2}}', '$.a'), '$.b', true)",
				shape('value' => '{"b":true}'),
			),
			tuple("JSON_REPLACE(JSON_EXTRACT(\"[false]\", '$[0]'), '$.a', 'test')", shape('value' => 'false')),

			tuple(
				"JSON_REPLACE('[0,1]', '$[1]', REPLACE(JSON_UNQUOTE(JSON_EXTRACT('{\"a\":{\"b\": \"test\"}}', '$.a.b')), 'te', 're'))",
				shape('value' => '[0,"rest"]'),
			),
			tuple("JSON_REPLACE('{\"b\":2}', '$.b', 1 < 2)", shape('value' => '{"b":true}')),

			// JSON value as replacement in JSON_REPLACE
			tuple(
				"JSON_REPLACE('[0,1]', '$[1]', JSON_EXTRACT('{\"a\": \"test\"}', '$.a'))",
				shape('value' => '[0,"test"]'),
			),
			tuple(
				"JSON_REPLACE('{\"a\":2}', JSON_UNQUOTE(JSON_EXTRACT('{\"b\":\"$.a\"}', '$.b')), 4)",
				shape('value' => '{"a":4}'),
			),
			tuple("JSON_UNQUOTE(JSON_EXTRACT('{\"a\": true}', '$.a'))", shape('value' => 'true')),
			tuple("JSON_UNQUOTE(JSON_EXTRACT('{\"a\": \"test\"}', '$.a'))", shape('value' => 'test')),
			tuple("JSON_UNQUOTE(JSON_EXTRACT('{\"a\": 5}', '$.a'))", shape('value' => '5')),

			// exceptional
			// JSON_EXTRACT output as JSON_QUOTE arg
			tuple(
				"JSON_QUOTE(JSON_EXTRACT('{\"a\":\"test\"}', '$.a'))",
				shape('exception' => SQLFakeRuntimeException::class),
			),
			// JSON as path for JSON_EXTRACT
			tuple(
				"JSON_EXTRACT('[true]', JSON_EXTRACT('[\"$[0]\"]', '$[0]'))",
				shape('exception' => SQLFakeRuntimeException::class),
			),
			// JSON as path for JSON_REPLACE
			tuple(
				"JSON_REPLACE('{\"a\":2}', JSON_EXTRACT('{\"b\":\"$.a\"}', '$.b'), 2)",
				shape('exception' => SQLFakeRuntimeException::class),
			),
		];
	}

	<<DataProvider('testJSONFunctionCompositionProvider')>>
	public async function testJSONFunctionComposition(
		string $select,
		JSONFunctionCompositionExpectedType $expected,
	): Awaitable<void> {
		await $this->simpleSelectTestCase($select, $expected);
	}

	public static async function testJSONContainsProvider(): Awaitable<vec<(string, JSONContainsExpectedType)>> {
		return vec[
			// NULL inputs
			tuple("JSON_CONTAINS(NULL, '{}', '$')", shape('value' => null)),
			tuple("JSON_CONTAINS('{}', NULL, '$')", shape('value' => null)),

			// bad input
			tuple("JSON_CONTAINS('{}')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_CONTAINS('', 2, '$')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_CONTAINS('{}', 2, '$', '$')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_CONTAINS('{}', '\$fd', 2)", shape('exception' => SQLFakeRuntimeException::class)),

			// non-existent path
			tuple("JSON_CONTAINS('null', '2', '$.a')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_CONTAINS('[]', 45, '$.a')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_CONTAINS('2', 45, '$.a')", shape('exception' => SQLFakeRuntimeException::class)),
			tuple("JSON_CONTAINS('{\"a\": {\"b\":\"test\"}}', '45', '$.b')", shape('exception' => SQLFakeRuntimeException::class)),

			// existent path - array
			tuple("JSON_CONTAINS('[]', '4', '$')", shape('value' => 0)),
			tuple("JSON_CONTAINS('[1, 2, 3, 4]', '1', '$')", shape('value' => 1)),
			tuple("JSON_CONTAINS('[\"blue\", \"green\", \"red\", \"yellow\"]', '\"red\"', '$')", shape('value' => 1)),

			// existent path - object
			tuple("JSON_CONTAINS('{}', '4', '$')", shape('value' => 0)),
			tuple("JSON_CONTAINS('{\"a\": {\"b\":\"test\"}}', '\"test\"', '$.a.b')", shape('value' => 1)),
			tuple("JSON_CONTAINS('{\"a\": {\"b\":\"test\"}}', '{\"b\":\"test\"}', '$.a')", shape('value' => 1)),
			tuple("JSON_CONTAINS('{\"a\": {\"b\":\"test\"}}', '\"test\"', '$.a')", shape('value' => 1)),

			// no path - array
			tuple("JSON_CONTAINS('[]', '4')", shape('value' => 0)),
			tuple("JSON_CONTAINS('[1, 2, 3, 4]', '1')", shape('value' => 1)),
			tuple("JSON_CONTAINS('[\"blue\", \"green\", \"red\", \"yellow\"]', '\"red\"')", shape('value' => 1)),

			// no path - object
			tuple("JSON_CONTAINS('{}', '4')", shape('value' => 0)),
			tuple("JSON_CONTAINS('{\"a\": {\"b\":\"test\"}}', '\"test\"')", shape('value' => 0)),
			tuple("JSON_CONTAINS('{\"a\": {\"b\":\"test\"}}', '{\"a\": {\"b\":\"test\"}}')", shape('value' => 1)),
			tuple("JSON_CONTAINS('{\"a\": {\"b\":\"test\"}}', '{\"b\":\"test\"}')", shape('value' => 1)),
		];
	}

	<<DataProvider('testJSONContainsProvider')>>
	public async function testJSONContains(string $select, JSONContainsExpectedType $expected): Awaitable<void> {
		await $this->simpleSelectTestCase($select, $expected);
	}

	private async function simpleSelectTestCase(
		string $select,
		shape(?'exception' => classname<SQLFakeException>, ?'value' => mixed) $expected,
	): Awaitable<void> {
		$exception = $expected['exception'] ?? null;

		$conn = static::$conn as nonnull;
		$sql = 'SELECT '.$select.' AS expected';

		if (!$exception) {
			$results = await $conn->query($sql);
			$expectedValue = $expected['value'] ?? null;

			expect($results->rows())->toBeSame(vec[dict['expected' => $expectedValue]], $sql);

			return;
		}

		expect(async () ==> await $conn->query($sql))->toThrow($exception);
	}
}
