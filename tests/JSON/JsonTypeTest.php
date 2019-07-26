<?hh // strict

namespace Slack\SQLFake;

use type Facebook\HackTest\{DataProvider, HackTest};
use function Facebook\FBExpect\expect;
use namespace Slack\SQLFake\JSON;

final class JsonTypeTest extends HackTest {

  public function provideCases(): vec<(string, ?JSON\JsonType)> {
    return vec[
      tuple('1', JSON\JsonType::INTEGER),
      tuple('1.1', JSON\JsonType::DECIMAL),
      tuple('[]', JSON\JsonType::ARRAY),
      tuple('[1, {}, {"a": "b"}, null]', JSON\JsonType::ARRAY),
      tuple('{}', JSON\JsonType::OBJECT),
      tuple('{"a": 1, "b": [], "c": {"a": "b"}, "d": null}', JSON\JsonType::OBJECT),
    ];
  }

  <<DataProvider('provideCases')>>
  public function testLazyMode(string $column_value, ?JSON\JsonType $expected): void {
    expect(JSON\type_lazy($column_value))->toBeSame($expected);
  }

  <<DataProvider('provideCases')>>
  public function testNonLazyMode(string $column_value, ?JSON\JsonType $expected): void {
    expect(JSON\type_lazy($column_value))->toBeSame($expected);
  }

  public function provideNullyCases(): vec<(?string, ?JSON\JsonType)> {
    return vec[
      tuple('null', JSON\JsonType::NULL),
      tuple(null, null),
    ];
  }

  <<DataProvider('provideNullyCases')>>
  public function testFull(?string $column_value, ?JSON\JsonType $expected): void {
    expect(JSON\type($column_value))->toBeSame($expected);
  }


}
