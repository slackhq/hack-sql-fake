<?hh

namespace Slack\SQLFake\_Private;

use type ConstVector;

// HHAST_IGNORE_ALL[CamelCasedMethodsUnderscoredFunctions] just implementing the interface

// It is a really nice idea to "implement" the hhi from HH\SQLxxxxxxx.
// This ensures that you do not forget anything.
// TIL that these interfaces are not present at runtime.
// This means that the typechecker will gladly validate your code
// when you uncomment the implements clause, but the runtime will
// fatal loudly. I'll file an issue against hhvm later today.

final class SQLEqualsScalarFormatter /* implements \HH\SQLScalarFormatter */ {
	public function __construct(private EqualsScalarFormat $scalarFormat) {}

	public function format_d(?int $int): string {
		return $this->scalarFormat->format_d($int);
	}
	public function format_f(?float $float): string {
		return $this->scalarFormat->format_f($float);
	}
	public function format_s(?string $string): string {
		return $this->scalarFormat->format_s($string);
	}
}

final class SQLListFormatter /* implements \HH\SQLListFormatter */ {
	public function __construct(private ListFormat $listFormat) {}

	public function format_upcase_c(ConstVector<string> $columns): string {
		return $this->listFormat->format_upcase_c(vec($columns));
	}
	public function format_d(ConstVector<int> $ints): string {
		return $this->listFormat->format_d(vec($ints));
	}
	public function format_f(ConstVector<float> $floats): string {
		return $this->listFormat->format_f(vec($floats));
	}
	public function format_s(ConstVector<string> $strings): string {
		return $this->listFormat->format_s(vec($strings));
	}
}

final class SQLFormatter /* implements \HH\SQLFormatter */ {
	public function __construct(private QueryFormat $queryFormat) {}
	public function format_0x25(): string {
		return '%';
	}
	public function format_d(?int $int): string {
		return $this->queryFormat->format_d($int);
	}
	public function format_f(?float $float): string {
		return $this->queryFormat->format_f($float);
	}
	public function format_s(?string $string): string {
		return $this->queryFormat->format_s($string);
	}
	public function format_upcase_t(string $table): string {
		return $this->queryFormat->format_upcase_t($table);
	}
	public function format_upcase_c(string $column): string {
		return $this->queryFormat->format_upcase_c($column);
	}
	public function format_upcase_l(): SQLListFormatter {
		return new SQLListFormatter(new ListFormat());
	}
	public function format_0x3d(): SQLEqualsScalarFormatter {
		return new SQLEqualsScalarFormatter(new EqualsScalarFormat());
	}
}
