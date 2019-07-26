<?hh // strict

namespace Slack\SQLFake;

use type Facebook\HackTest\{DataProvider, HackTest};
use function Facebook\FBExpect\expect;
use namespace Slack\SQLFake\JSON;

final class JsonLengthTest extends HackTest {

	public function provideCases(): vec<(?string, ?int)> {
		return vec[
			tuple(null, null),
			tuple('null', 0),
			tuple('1', 1),
			tuple('"string"', 1),
			tuple('[]', 0),
			tuple('[1, 2, 3, [4, ["still 4"]], 5]', 5),
			tuple('{}', 0),
			tuple('{"1": 1, "2": [2, ["still 2"]], "3": {"still 3": "3"}, "four": 4}', 4),
		];
	}

	<<DataProvider('provideCases')>>
	public function testForCorrectness(?string $column_value, ?int $expected): void {
		expect(JSON\length($column_value))->toBeSame($expected);
	}

	public function testInvalidJsonArgument(): void {
		expect(() ==> JSON\length(''))->toThrow(\InvalidArgumentException::class);
	}


}
