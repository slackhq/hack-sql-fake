namespace Slack\SQLFake;

class VitessSharding {
	public function __construct(public string $keyspace, public string $sharding_key) {}
}
