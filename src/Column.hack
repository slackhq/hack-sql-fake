namespace Slack\SQLFake;

final class Column {
	public function __construct(
		public string $name,
		public DataType $type,
		public int $length,
		public bool $null,
		public string $hack_type,
		public ?bool $unsigned = null,
		public ?string $default = null,
	) {}
}
