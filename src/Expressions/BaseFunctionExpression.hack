namespace Slack\SQLFake;

use namespace HH\Lib\C;

/**
 * Abstract base class for function expressions
 */
abstract class BaseFunctionExpression extends Expression {

	protected string $functionName;
	protected bool $evaluatesGroups = true;

	public function __construct(private token $token, protected vec<Expression> $args, protected bool $distinct) {
		$this->type = $token['type'];
		$this->precedence = 0;
		$this->functionName = $token['value'];
		$this->name = $token['value'];
		/*HH_FIXME[4110] Open issue #24 should resolve what to do here.*/
		$this->operator = (string)$this->type;
	}

	public function functionName(): string {
		return $this->functionName;
	}

	<<__Override>>
	public function isWellFormed(): bool {
		return true;
	}

	/**
	 * helper for functions which take one expression as an argument
	 */
	protected function getExpr(): Expression {
		invariant(C\count($this->args) === 1, 'expression must have one argument');
		return C\firstx($this->args);
	}

	<<__Override>>
	public function __debugInfo(): dict<string, mixed> {
		$args = vec[];
		foreach ($this->args as $arg) {
			$args[] = \var_dump($arg, true);
		}
		return dict[
			'type' => (string)$this->type,
			'functionName' => $this->functionName,
			'args' => $args,
			'name' => $this->name,
			'distinct' => $this->distinct,
		];
	}
}
