namespace Slack\SQLFake;

use function Facebook\FBExpect\expect;
use type Facebook\HackTest\{DataProvider, HackTest};
use namespace HH\Lib\{C, Str};

final class JSONFunctionTest extends HackTest {
    private static ?AsyncMysqlConnection $conn;

    <<__Override>>
    public static async function beforeFirstTestAsync(): Awaitable<void> {
        static::$conn = await SharedSetup::initAsync();
        // block hole logging
        Logger::setHandle(new \Facebook\CLILib\TestLib\StringOutput());
    }

    <<__Override>>
    public async function beforeEachTestAsync(): Awaitable<void> {
        restore('setup');
        QueryContext::$strictSchemaMode = false;
        QueryContext::$strictSQLMode = false;
    }

    public static async function testJSONValidProvider(): Awaitable<vec<mixed>> {
        return vec[
            // Valid
            tuple("'null'", 1),
            tuple("'{\"key\": \"value\"}'", 1),
            tuple("'[{\"key\": \"value\"}]'", 1),
            tuple("'\"\"'", 1),
            tuple("'true'", 1),
            tuple("'false'", 1),
            tuple("'2'", 1),

            // Invalid
            tuple("' '", 0),
            tuple("'arbitrary_string'", 0),
            tuple('NULL', null),
            tuple('2', 0),
            tuple('TRUE', 0),
        ];
    }

    <<DataProvider('testJSONValidProvider')>>
    public async function testJSONValid(string $json, ?int $expected): Awaitable<void> {
        $conn = static::$conn as nonnull;
        $results = await $conn->query("SELECT JSON_VALID($json) as valid");

        $expectedString = $expected is null ? 'NULL' : (string)$expected;
        expect($results->rows())->toBeSame(vec[dict['valid' => $expected]], "JSON_VALID({$json}) => {$expectedString}");
    }

    public static async function testJSONQuoteProvider(): Awaitable<vec<mixed>> {
        return vec[
            tuple('null', shape('value' => null)),
            tuple("'null'", shape('value' => '"null"')),
            tuple("'a'", shape('value' => '"a"')),
            tuple("'{\"a\":\"c\"}'", shape('value' => '"{\"a\":\"c\"}"')),
            tuple("'[1, \"\"\"]'", shape('value' => '"[1, \"\"\"]"')),
            tuple(
                "'[1, \"\\\\\"]'",
                shape('value' => '"[1, \"\\\\\"]"'),
            ), // In PHP \\ represents \ in single & double quoted strings
            tuple("'►'", shape('value' => '"►"')), // MySQL doesn't seem to escape these
            tuple("'2\n2'", shape('value' => '"2\\n2"')), // Escapes newline
            tuple("'".'22'.\chr(8)."'", shape('value' => '"22\\b"')), // Escapes backspace character

            // invalid
            tuple('TRUE', shape('exceptional' => true)),
            tuple('45', shape('exceptional' => true)),
        ];
    }

    <<DataProvider('testJSONQuoteProvider')>>
    public async function testJSONQuote(
        string $input,
        shape(?'exceptional' => bool, ?'value' => ?string) $output,
    ): Awaitable<void> {
        $exceptional = $output['exceptional'] ?? false;

        $conn = static::$conn as nonnull;

        $sql = "SELECT JSON_QUOTE({$input}) as quoted";

        if (!$exceptional) {
            invariant(Shapes::keyExists($output, 'value'), 'expected value must be present in non-exceptional cases');
            $results = await $conn->query($sql);
            $expectedString = $output['value'] is null ? 'NULL' : $output['value'];
            expect($results->rows())->toBeSame(
                vec[dict['quoted' => $output['value']]],
                "JSON_QUOTE({$input}) => {$expectedString}",
            );
            return;
        }

        expect(async () ==> await $conn->query($sql))->toThrow(SQLFakeRuntimeException::class);
    }

    public static async function testJSONUnquoteProvider(): Awaitable<vec<mixed>> {
        return vec[
            tuple('null', shape('value' => null)),
            tuple('"null"', shape('value' => 'null')),
            tuple('"a"', shape('value' => 'a')),
            tuple("'\\\\'", shape('value' => '\\')), // no-op
            tuple('\'"{\\\\\"a\\\\\":\\\\\"c\\\\\"}"\'', shape('value' => '{"a":"c"}')),
            tuple('\'[1, """]\'', shape('value' => '[1, """]')), // no-op
            tuple('\'"[1, \\\"\\\"\\\"]"\'', shape('value' => '[1, """]')),
            tuple('\'"\\\\\\\\"\'', shape('value' => '\\')),
            tuple('\'"\\\u25ba"\'', shape('value' => '►')),
            tuple('\'"\\\\n"\'', shape('value' => "\n")),
            tuple('\'"2\\\\b"\'', shape('value' => '2'.\chr(8))),

            // invalid
            tuple('\'"\\\\"\'', shape('exceptional' => true)), // inner function receive '"\"'
            tuple('2', shape('exceptional' => true)),
        ];
    }

    <<DataProvider('testJSONUnquoteProvider')>>
    public async function testJSONUnquote(
        string $input,
        shape(?'exceptional' => bool, ?'value' => ?string) $output,
    ): Awaitable<void> {
        $exceptional = $output['exceptional'] ?? false;

        $conn = static::$conn as nonnull;

        $sql = "SELECT JSON_UNQUOTE({$input}) as unquoted";

        if (!$exceptional) {
            invariant(Shapes::keyExists($output, 'value'), 'expected value must be present in non-exceptional cases');
            $results = await $conn->query($sql);
            $expectedString = $output['value'] is null ? 'NULL' : $output['value'];
            expect($results->rows())->toBeSame(
                vec[dict['unquoted' => $output['value']]],
                "{$input} => {$expectedString}",
            );
            return;
        }

        expect(async () ==> await $conn->query($sql))->toThrow(SQLFakeRuntimeException::class);
    }

    public static async function testJSONExtractProvider(): Awaitable<vec<mixed>> {
        return vec[
            // valid
            tuple(shape('json' => null, 'paths' => vec["'$.a'"]), shape('value' => null)),
            tuple(shape('json' => null, 'paths' => vec["'$.a'"]), shape('value' => null)),
            tuple(shape('json' => '{"a": {"b": "test"}}', 'paths' => vec['NULL']), shape('value' => null)),
            tuple(shape('json' => '{"a": {"b": "test"}}', 'paths' => vec["'$.a.b'"]), shape('value' => '"test"')),
            tuple(shape('json' => '{"a": {"b": 2}}', 'paths' => vec["'$.a.b'"]), shape('value' => '2')),
            tuple(shape('json' => '{"a": {"b": true}}', 'paths' => vec["'$.a.b'"]), shape('value' => 'true')),
            tuple(
                shape('json' => '{"a": {"b": 2, "c": "test"}}', 'paths' => vec["'$.a.b'", '\'$."a"."c"\'']),
                shape('value' => '[2,"test"]'),
            ),
            tuple(
                shape('json' => '[{"a": [{"b": "test"}]}]', 'paths' => vec["'$[0].a'"]),
                shape('value' => '[{"b":"test"}]'),
            ),
            tuple(
                shape('json' => '[{"a": [{"b": "test"}]}]', 'paths' => vec["'$[0].a[0]'"]),
                shape('value' => '{"b":"test"}'),
            ),
            tuple(
                shape('json' => '[{"a": [{"b": "test"}]}]', 'paths' => vec['\'$**."b"\'']),
                shape('value' => '["test"]'),
            ),
            tuple(
                shape('json' => '[{"a": [{"b": "test", "c":false}]}]', 'paths' => vec["'$**.*'"]),
                shape('value' => '[[{"b":"test","c":false}],"test",false]'),
            ),
            tuple(shape('json' => '{"a": {"b": ["test",10]}}', 'paths' => vec["'$**[1]'"]), shape('value' => '[10]')),
            tuple(shape('json' => '2', 'paths' => vec["'$**[1]'"]), shape('value' => null)),
            tuple(shape('json' => '{"a":2}', 'paths' => vec["'$.a'", "'$.b'"]), shape('value' => '[2]')),
            tuple(shape('json' => '"a"', 'paths' => vec["'$'"]), shape('value' => '"a"')),

            // non-existent
            tuple(shape('json' => '{"a": 2}', 'paths' => vec["'$.b'"]), shape('value' => null)),
            tuple(shape('json' => '{"a": 2}', 'paths' => vec["'$.b'", "'$[0]'"]), shape('value' => null)),

            // invalid
            tuple(shape('json' => '[}', 'paths' => vec["'\$a.b'"]), shape('exceptional' => true)),
            tuple(shape('json' => '[]', 'paths' => vec["'a.b'"]), shape('exceptional' => true)),
            tuple(shape('json' => '[]', 'paths' => vec['2']), shape('exceptional' => true)),
            tuple(shape('json' => '[]', 'paths' => vec['true']), shape('exceptional' => true)),
        ];
    }

    <<DataProvider('testJSONExtractProvider')>>
    public async function testJSONExtract(
        shape('json' => ?string, 'paths' => vec<string>) $input,
        shape(?'value' => string, ?'exceptional' => bool) $output,
    ): Awaitable<void> {
        $exceptional = $output['exceptional'] ?? false;
        $conn = static::$conn as nonnull;

        $pathsSql = Str\join($input['paths'], ', ');

        $json = $input['json'] is null ? 'NULL' : "'{$input['json']}'";
        $sql = "SELECT JSON_EXTRACT({$json}, {$pathsSql}) as extracted";

        if (!$exceptional) {
            invariant(Shapes::keyExists($output, 'value'), 'expected value must be present in non-exceptional cases');
            $results = await $conn->query($sql);
            expect($results->rows())->toBeSame(vec[dict['extracted' => $output['value']]]);
            return;
        }

        expect(async () ==> await $conn->query($sql))->toThrow(SQLFakeRuntimeException::class);
    }

    public static async function testJSONReplaceProvider(): Awaitable<vec<mixed>> {
        return vec[
            tuple(
                shape('json' => 'null', 'replacements' => vec[shape('path' => "'$.a'", 'value' => '2')]),
                shape('value' => null),
            ),
            tuple(
                shape('json' => "'{}'", 'replacements' => vec[shape('path' => 'NULL', 'value' => '2')]),
                shape('value' => null),
            ),
            tuple(
                shape('json' => "'[1]'", 'replacements' => vec[shape('path' => "'$[0]'", 'value' => 'NULL')]),
                shape('value' => '[null]'),
            ),
            tuple(
                shape('json' => "'null'", 'replacements' => vec[shape('path' => "'$.a'", 'value' => '2')]),
                shape('value' => 'null'),
            ),
            tuple(
                shape(
                    'json' => "'{\"a\":{\"b\":\"test\"}}'",
                    'replacements' => vec[shape('path' => "'$.a.b'", 'value' => '2')],
                ),
                shape('value' => '{"a":{"b":2}}'),
            ),
            tuple(
                shape(
                    'json' => "'2'",
                    'replacements' => vec[shape('path' => "'$.a'", 'value' => '45')],
                ),
                shape('value' => '2'),
            ),
            // no-op
            tuple(
                shape(
                    'json' => "'{\"a\": {\"b\":\"test\"}}'",
                    'replacements' => vec[shape('path' => "'$.b'", 'value' => '45')],
                ),
                shape('value' => '{"a":{"b":"test"}}'),
            ),
            // multiple
            tuple(
                shape(
                    'json' => "'{\"a\": [1,2]}'",
                    'replacements' => vec[
                        shape('path' => "'$.a[0]'", 'value' => '3'),
                        shape('path' => "'$.a[1]'", 'value' => '4'),
                    ],
                ),
                shape('value' => '{"a":[3,4]}'),
            ),
            // successive replacements work off interim object
            tuple(
                shape(
                    'json' => "'{\"a\": [1,2]}'",
                    'replacements' => vec[
                        shape('path' => "'$.a[0]'", 'value' => '3'),
                        shape('path' => "'$.a[0]'", 'value' => '4'),
                    ],
                ),
                shape('value' => '{"a":[4,2]}'),
            ),
        ];
    }

    <<DataProvider('testJSONReplaceProvider')>>
    public async function testJSONReplace(
        shape('json' => string, 'replacements' => vec<shape('path' => string, 'value' => string)>) $input,
        shape(?'exceptional' => bool, ?'value' => string) $output,
    ): Awaitable<void> {
        $exceptional = $output['exceptional'] ?? false;
        $conn = static::$conn as nonnull;

        $replacementsSql = C\reduce(
            $input['replacements'],
            ($acc, $r) ==> {
                $current = Str\format('%s, %s', $r['path'], $r['value']);
                return !Str\is_empty($acc) ? Str\format('%s, %s', $acc, $current) : $current;
            },
            '',
        );

        $sql = "SELECT JSON_REPLACE({$input['json']}, {$replacementsSql}) as replaced";

        if (!$exceptional) {
            invariant(Shapes::keyExists($output, 'value'), 'expected value must be present in non-exceptional cases');
            $results = await $conn->query($sql);
            expect($results->rows())->toBeSame(vec[dict['replaced' => $output['value']]]);
            return;
        }

        expect(async () ==> await $conn->query($sql))->toThrow(SQLFakeRuntimeException::class);
    }

    public static async function testJSONKeysProvider(): Awaitable<vec<mixed>> {
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
    public async function testJSONKeys(
        string $select,
        shape(?'exception' => classname<SQLFakeException>, ?'value' => ?string) $expected,
    ): Awaitable<void> {
        $exception = $expected['exception'] ?? null;

        $conn = static::$conn as nonnull;
        $sql = 'SELECT '.$select.' AS keys';

        if (!$exception) {
            $results = await $conn->query($sql);

            $expectedValue = $expected['value'] ?? null;
            if ($expectedValue is null) {
                expect($results->rows())->toBeSame(vec[dict['keys' => null]]);
            } else {
                expect($results->rows())->toBeSame(vec[dict['keys' => $expectedValue]]);
            }

            return;
        }

        expect(async () ==> await $conn->query($sql))->toThrow($exception);
    }

    public static async function testJSONLengthProvider(): Awaitable<vec<mixed>> {
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
    public async function testJSONLength(
        string $select,
        shape(?'exception' => classname<SQLFakeException>, ?'value' => ?int) $expected,
    ): Awaitable<void> {
        $exception = $expected['exception'] ?? null;

        $conn = static::$conn as nonnull;
        $sql = 'SELECT '.$select.' AS length';

        if (!$exception) {
            $results = await $conn->query($sql);

            $expectedValue = $expected['value'] ?? null;
            if ($expectedValue is null) {
                expect($results->rows())->toBeSame(vec[dict['length' => null]]);
            } else {
                expect($results->rows())->toBeSame(vec[dict['length' => $expectedValue]]);
            }

            return;
        }

        expect(async () ==> await $conn->query($sql))->toThrow($exception);
    }

    public static async function testJSONFunctionCompositionProvider(): Awaitable<vec<mixed>> {
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
            tuple("JSON_QUOTE(JSON_EXTRACT('{\"a\":\"test\"}', '$.a'))", shape('exceptional' => true)),
            // JSON as path for JSON_EXTRACT
            tuple("JSON_EXTRACT('[true]', JSON_EXTRACT('[\"$[0]\"]', '$[0]'))", shape('exceptional' => true)),
            // JSON as path for JSON_REPLACE
            tuple("JSON_REPLACE('{\"a\":2}', JSON_EXTRACT('{\"b\":\"$.a\"}', '$.b'), 2)", shape('exceptional' => true)),
        ];
    }

    <<DataProvider('testJSONFunctionCompositionProvider')>>
    public async function testJSONFunctionComposition(
        string $sql,
        shape(?'exceptional' => bool, ?'value' => mixed) $output,
    ): Awaitable<void> {
        $exceptional = $output['exceptional'] ?? false;

        $conn = static::$conn as nonnull;
        $sql = 'SELECT '.$sql.' AS expected';

        if (!$exceptional) {
            invariant(Shapes::keyExists($output, 'value'), 'expected value must be present in non-exceptional cases');
            $results = await $conn->query($sql);
            expect($results->rows())->toBeSame(vec[dict['expected' => $output['value']]]);
            return;
        }

        expect(async () ==> await $conn->query($sql))->toThrow(SQLFakeRuntimeException::class);
    }
}
