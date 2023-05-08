<?hh // strict

namespace Slack\SQLFake;

use function Facebook\FBExpect\expect;
use type Facebook\HackTest\HackTest;

// most of the test cases are inspired by
// https://github.com/vitessio/vitess/blob/master/go/vt/vtgate/planbuilder/testdata/unsupported_cases.txt

final class SelectQueryValidatorTest extends HackTest {
	private static ?AsyncMysqlConnection $conn;

	<<__Override>>
	public static async function beforeFirstTestAsync(): Awaitable<void> {
		static::$conn = await SharedSetup::initVitessAsync();
		// block hole logging
		Logger::setHandle(new \HH\Lib\IO\MemoryHandle());
	}

	<<__Override>>
	public async function beforeEachTestAsync(): Awaitable<void> {
		restore('vitess_setup');
		QueryContext::$strictSchemaMode = false;
		QueryContext::$strictSQLMode = false;
	}

	public async function testScatterByColumns(): Awaitable<void> {
		$conn = static::$conn as nonnull;

		$unsupported_test_cases = vec[
			"select * from vt_table1 where id=2 and name='hi' group by id",
			'select * from vt_table1 group by name',
		];
		foreach ($unsupported_test_cases as $sql) {
			expect(() ==> $conn->query($sql))->toThrow(
				SQLFakeVitessQueryViolation::class,
				'Vitess query validation error: unsupported: in scatter query: group by column must reference column in SELECT list',
			);
		}

		$unsupported_test_cases = vec[
			'select * from vt_table1 where id in (1, 2) order by id',
			'select id from vt_table1 order by name',
			'select id, count(*) from vt_table1 group by id order by c1',
		];
		$supported_test_cases = vec[
			'select * from vt_table1 where id=2 order by name',
			"select * from vt_table1 where id=2 and name='bob' order by name,id",
		];
		foreach ($unsupported_test_cases as $sql) {
			expect(() ==> $conn->query($sql))->toThrow(
				SQLFakeVitessQueryViolation::class,
				'Vitess query validation error: unsupported: in scatter query: order by column must reference column in SELECT list',
			);
		}
		foreach ($supported_test_cases as $sql) {
			expect(() ==> $conn->query($sql))->notToThrow(SQLFakeVitessQueryViolation::class);
		}
	}

	public async function testUnionsNotAllowed(): Awaitable<void> {
		$conn = static::$conn as nonnull;

		$test_cases = vec[
			'select * from vt_table1 union select * from vt_table2',
			'select id from vt_table1 union all select id from vt_table2',
		];

		foreach ($test_cases as $sql) {
			expect(() ==> $conn->query($sql))->toThrow(
				SQLFakeVitessQueryViolation::class,
				'Vitess query validation error: unsupported: UNION cannot be executed as a single route',
			);
		}
	}
}
