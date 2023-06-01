namespace Slack\SQLFake;

final class Index {
	public function __construct(
		public string $name,
		public string $type,
		public keyset<string> $fields,
		public bool $vitess_sharding_key = false,
	) {}
}
