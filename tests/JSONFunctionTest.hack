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
            tuple('null', null),
            tuple("'null'", '"null"'),
            tuple("'a'", '"a"'),
            tuple("'{\"a\":\"c\"}'", '"{\"a\":\"c\"}"'),
            tuple("'[1, \"\"\"]'", '"[1, \"\"\"]"'),
            tuple("'[1, \"\\\\\"]'", '"[1, \"\\\\\"]"'), // In PHP \\ represents \ in single & double quoted strings
            tuple("'►'", '"►"'), // MySQL doesn't seem to escape these
            tuple("'2\n2'", '"2\\n2"'), // Escapes newline
            tuple("'".'22'.\chr(8)."'", '"22\\b"'), // Escapes backspace character
        ];
    }

    <<DataProvider('testJSONQuoteProvider')>>
    public async function testJSONQuote(string $input, ?string $expected): Awaitable<void> {
        $conn = static::$conn as nonnull;
        $results = await $conn->query("SELECT JSON_QUOTE({$input}) as quoted");

        $expectedString = $expected is null ? 'NULL' : $expected;
        expect($results->rows())->toBeSame(
            vec[dict['quoted' => $expected]],
            "JSON_QUOTE({$input}) => {$expectedString}",
        );
    }

    public static async function testJSONUnquoteProvider(): Awaitable<vec<mixed>> {
        return vec[
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
        shape(?'exceptional' => bool, ?'value' => string) $output,
    ): Awaitable<void> {
        $exceptional = $output['exceptional'] ?? false;

        $conn = static::$conn as nonnull;

        $sql = "SELECT JSON_UNQUOTE({$input}) as unquoted";

        if (!$exceptional) {
            invariant(Shapes::keyExists($output, 'value'), 'expected value must be present in non-exceptional cases');
            $results = await $conn->query($sql);
            expect($results->rows())->toBeSame(
                vec[dict['unquoted' => $output['value']]],
                "{$input} => {$output['value']}",
            );
            return;
        }

        $exceptionThrown = false;
        try {
            $results = await $conn->query($sql);
        } catch (SQLFakeRuntimeException $e) {
            $exceptionThrown = true;
        }
        expect($exceptionThrown)->toBeTrue();
    }

    public static async function testJSONExtractProvider(): Awaitable<vec<mixed>> {
        return vec[
            // valid
            tuple(shape('json' => '{"a": {"b": "test"}}', 'paths' => vec["'$.a.b'"]), shape('value' => '"test"')),
            tuple(shape('json' => '{"a": {"b": 2}}', 'paths' => vec["'$.a.b'"]), shape('value' => 2)),
            tuple(shape('json' => '{"a": {"b": true}}', 'paths' => vec["'$.a.b'"]), shape('value' => 1)),
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
        shape('json' => string, 'paths' => vec<string>) $input,
        shape(?'value' => string, ?'exceptional' => bool) $output,
    ): Awaitable<void> {
        $exceptional = $output['exceptional'] ?? false;
        $conn = static::$conn as nonnull;

        $pathsSql = Str\join($input['paths'], ', ');
        $sql = "SELECT JSON_EXTRACT('{$input['json']}', {$pathsSql}) as extracted";

        if (!$exceptional) {
            invariant(Shapes::keyExists($output, 'value'), 'expected value must be present in non-exceptional cases');
            $results = await $conn->query($sql);
            expect($results->rows())->toBeSame(vec[dict['extracted' => $output['value']]]);
            return;
        }

        $exceptionThrown = false;
        try {
            $results = await $conn->query($sql);
        } catch (SQLFakeRuntimeException $e) {
            $exceptionThrown = true;
        }
        expect($exceptionThrown)->toBeTrue();
    }

    public static async function testJSONReplaceProvider(): Awaitable<vec<mixed>> {
        return vec[
            tuple(
                shape(
                    'json' => '{"a":{"b":"test"}}',
                    'replacements' => vec[shape('path' => '$.a.b', 'value' => 2)],
                ),
                shape('value' => '{"a":{"b":2}}'),
            ),
            tuple(
                shape(
                    'json' => '2',
                    'replacements' => vec[shape('path' => '$.a', 'value' => 45)],
                ),
                shape('value' => 2),
            ),
            // no-op
            tuple(
                shape(
                    'json' => '{"a": {"b":"test"}}',
                    'replacements' => vec[shape('path' => '$.b', 'value' => 45)],
                ),
                shape('value' => '{"a":{"b":"test"}}'),
            ),
            // multiple
            tuple(
                shape(
                    'json' => '{"a": [1,2]}',
                    'replacements' => vec[
                        shape('path' => '$.a[0]', 'value' => '3'),
                        shape('path' => '$.a[1]', 'value' => '4'),
                    ],
                ),
                shape('value' => '{"a":[3,4]}'),
            ),
            // successive replacements work off interim object
            tuple(
                shape(
                    'json' => '{"a": [1,2]}',
                    'replacements' => vec[
                        shape('path' => '$.a[0]', 'value' => '3'),
                        shape('path' => '$.a[0]', 'value' => '4'),
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
                $current = Str\format('\'%s\', %s', $r['path'], $r['value']);
                return !Str\is_empty($acc) ? Str\format('%s, %s', $acc, $current) : $current;
            },
            '',
        );
        $sql = "SELECT JSON_REPLACE('{$input['json']}', {$replacementsSql}) as replaced";

        if (!$exceptional) {
            invariant(Shapes::keyExists($output, 'value'), 'expected value must be present in non-exceptional cases');
            $results = await $conn->query($sql);
            expect($results->rows())->toBeSame(vec[dict['replaced' => $output['value']]]);
            return;
        }

        $exceptionThrown = false;
        try {
            $results = await $conn->query($sql);
        } catch (SQLFakeRuntimeException $e) {
            $exceptionThrown = true;
        }
        expect($exceptionThrown)->toBeTrue();
    }

    public static async function testJSONFunctionCompositionProvider(): Awaitable<vec<mixed>> {
        return vec[
            tuple("JSON_EXTRACT(JSON_EXTRACT('{\"a\":true}', '$'), '$.a')", shape('value' => 1)),
            tuple(
                "JSON_REPLACE(JSON_EXTRACT('{\"a\":{\"b\": 2}}', '$.a'), '$.b', true)",
                shape('value' => '{"b":true}'),
            ),
            tuple(
                "JSON_REPLACE('[0,1]', '$[1]', REPLACE(JSON_UNQUOTE(JSON_EXTRACT('{\"a\":{\"b\": \"test\"}}', '$.a.b')), 'te', 're'))",
                shape('value' => '[0,"rest"]'),
            ),
            tuple("JSON_REPLACE('{\"b\":2}', '$.b', 1 < 2)", shape('value' => '{"b":true}')),
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
            tuple(
                "JSON_QUOTE(JSON_EXTRACT('{\"a\":\"test\"}', '$.a'))", // JSON_EXTRACT output as JSON_QUOTE arg
                shape('exceptional' => true),
            ),
            tuple(
                "JSON_REPLACE('{\"a\":2}', JSON_EXTRACT('{\"b\":\"$.a\"}', '$.b'), 2)", //JSON_REPLACE path doesn't support JSON_EXTRACT
                shape('exceptional' => true),
            ),
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

        $exceptionThrown = false;
        try {
            $results = await $conn->query($sql);
        } catch (SQLFakeRuntimeException $e) {
            $exceptionThrown = true;
        }
        expect($exceptionThrown)->toBeTrue('exception was not thrown');
    }
}
