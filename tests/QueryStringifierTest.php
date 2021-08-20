<?hh // strict

namespace Slack\SQLFake;

use type Facebook\HackTest\{DataProvider, HackTest};
use function Facebook\FBExpect\expect;
use namespace HH\Lib\SQL;

final class QueryStringifierTest extends HackTest {
	public static function provideValidQueries(): vec<(SQL\Query, string)> {
		return vec[
			tuple(new SQL\Query('Hello, World!'), 'Hello, World!'),
			tuple(new SQL\Query('5 %% 3'), '5 % 3'),
			tuple(new SQL\Query('SELECT %d', 5), 'SELECT 5'),
			tuple(new SQL\Query('SELECT %f', 3.1415926), 'SELECT 3.1415926'),
			tuple(new SQL\Query('SELECT %f', (float)(3 * 10 ** 20)), 'SELECT 300000000000000000000.'),
			tuple(new SQL\Query('SELECT %s', '"'), 'SELECT "\\""'),
			tuple(new SQL\Query('SELECT %s', '\\'), 'SELECT "\\\\"'),
			tuple(new SQL\Query('SELECT %C FROM %T', 'col', 'table'), 'SELECT `col` FROM `table`'),
			tuple(
				new SQL\Query('SELECT %C FROM %T', 'evil`; SLEEP(1); --', 'table'),
				'SELECT `evil``; SLEEP(1); --` FROM `table`',
			),
			tuple(new SQL\Query('%K SELECT 1', 'pool(high_perf)'), '/*pool(high_perf)*/ SELECT 1'),
			tuple(new SQL\Query('SELECT %Ld', vec[]), 'SELECT '),
			tuple(new SQL\Query('SELECT %Ld', vec[1, 2, 42]), 'SELECT 1, 2, 42'),
			tuple(new SQL\Query('SELECT %Lf', vec[]), 'SELECT '),
			tuple(new SQL\Query('SELECT %Lf', vec[1.2, 2.1, 42.42]), 'SELECT 1.2, 2.1, 42.42'),
			tuple(new SQL\Query('SELECT %Ls', vec[]), 'SELECT '),
			tuple(new SQL\Query('SELECT %Ls', vec['a', '"', '\\']), 'SELECT "a", "\\"", "\\\\"'),
			tuple(new SQL\Query('SELECT %LC FROM %T', vec['col', 'ev`il'], 't'), 'SELECT `col`, `ev``il` FROM `t`'),
			tuple(new SQL\Query('SELECT %C %=d', 'c', null), 'SELECT `c`  IS NULL'),
			tuple(new SQL\Query('SELECT %C %=d', 'c', 5), 'SELECT `c`  = 5'),
			tuple(new SQL\Query('SELECT %C %=f', 'c', null), 'SELECT `c`  IS NULL'),
			tuple(new SQL\Query('SELECT %C %=f', 'c', 53.35), 'SELECT `c`  = 53.35'),
			tuple(new SQL\Query('SELECT %C %=s', 'c', null), 'SELECT `c`  IS NULL'),
			tuple(new SQL\Query('SELECT %C %=s', 'c', '"; --'), 'SELECT `c`  = "\\"; --"'),
			tuple(
				new SQL\Query('SELECT * FROM %Q', new SQL\Query('%T', 'your_imagination')),
				'SELECT * FROM `your_imagination`',
			),
			tuple(
				new SQL\Query(
					'%K SELECT %LC FROM %T WHERE %C %=d %% 8 AND %C %=s AND %C IN(%Ld) ORDER BY %Q',
					'advanced',
					vec['a', 'b', 'c'],
					't',
					'd',
					5,
					'e',
					null,
					'f',
					vec[1, 2, 3],
					new SQL\Query('%C, %C DESC', 'a', 'b'),
				),
				'/*advanced*/ SELECT `a`, `b`, `c` FROM `t` WHERE `d`  = 5 % 8 AND `e`  IS NULL AND `f` IN(1, 2, 3) ORDER BY `a`, `b` DESC',
			),
		];
	}

	<<DataProvider('provideValidQueries')>>
	public function testCorrectStringification(SQL\Query $query, string $expected): void {
		expect(QueryStringifier::createForTypesafeHack()->formatQuery($query))->toEqual($expected);
	}

	public function provideInvalidQueries(): vec<(SQL\Query, string)> {
		return vec[
			tuple(new SQL\Query('SELECT "dangerous character"'), 'Saw dangerous character " in SQL query.'),
			tuple(new SQL\Query("SELECT 'dangerous character'"), 'Saw dangerous character \' in SQL query.'),
			tuple(new SQL\Query('SELECT `dangerous character`'), 'Saw dangerous character ` in SQL query.'),
			tuple(new SQL\Query('SELECT ;'), 'Saw dangerous character ; in SQL query.'),
			tuple(
				/* HH_FIXME[4038] no format_eof() method */
				new SQL\Query('SELECT %'),
				"Invalid format specifier, got ''",
			),
			tuple(
				/* HH_FIXME[4038] format_upcase_b() method */
				new SQL\Query('SELECT %B FROM'),
				"Invalid format specifier, got 'B '",
			),
			tuple(
				new SQL\Query('SELECT %s', static::tellALie<string>(3)),
				'Expected ?string for specifier %s at index 0, got integer. Query: SELECT %s',
			),
			tuple(
				new SQL\Query('SELECT %Ls', static::tellALie<vec<string>>('not a vec')),
				'Expected vec<string> for specifier %Ls at index 0, got string. Query: SELECT %Ls',
			),
			tuple(
				new SQL\Query('SELECT %Ls', static::tellALie<vec<string>>(vec[null])),
				'Expected all elements of vec to be string for specifier %Ls at index 0, got NULL. Query: SELECT %Ls',
			),
			tuple(
				/* HH_FIXME[4105] Too many arguments */
				new SQL\Query('no modifiers', 'an argument'),
				'Too many arguments provided, got 1, expected 0. Format: no modifiers',
			),
			tuple(
				/* HH_FIXME[4105] Too many arguments */
				new SQL\Query('one %s modifier', 'ok', 'bad'),
				'Too many arguments provided, got 2, expected 1. Format: one %s modifier',
			),
			tuple(
				/* HH_FIXME[4104] Too few arguments */
				new SQL\Query('one %s modifier'),
				'Too few arguments provided, got 0. Format: one %s modifier',
			),
		];
	}

	<<DataProvider('provideInvalidQueries')>>
	public function testRuntimeGuarding(SQL\Query $query, string $exception_message): void {
		try {
			QueryStringifier::createForTypesafeHack()->formatQuery($query);
			expect(false)->toBeTrue('Expected an exception, got none');
		} catch (SQLFakeParseException $e) {
			do {
				$message = $e->getMessage();
				$e = $e->getPrevious();
			} while ($e is nonnull);
			expect($message)->toContainSubstring($exception_message);
		}
	}

	private static function tellALie<T>(mixed $mixed): T {
		/* HH_FIXME[4110] We are telling lies intentionally. */
		return $mixed;
	}
}
