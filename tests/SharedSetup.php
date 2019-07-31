<?hh // strict

namespace Slack\SQLFake;
use namespace HH\Lib\Dict;

final class SharedSetup {
	public static async function initAsync(bool $is_vitess_conn = false): Awaitable<AsyncMysqlConnection> {
		$schema = TEST_SCHEMA;
		if ($is_vitess_conn) {
			$schema = Dict\merge($schema, VITESS_TEST_SCHEMA);
		}

		init($schema, true);
		$pool = new AsyncMysqlConnectionPool(darray[]);
		$conn = await $pool->connect("example", 1, 'db2', '', '');

		// populate database state
		$database = dict[
			'table3' => vec[
				dict['id' => 1, 'group_id' => 12345, 'name' => 'name1'],
				dict['id' => 2, 'group_id' => 12345, 'name' => 'name2'],
				dict['id' => 3, 'group_id' => 12345, 'name' => 'name3'],
				dict['id' => 4, 'group_id' => 6, 'name' => 'name3'],
				dict['id' => 6, 'group_id' => 6, 'name' => 'name3'],
			],
			'table4' => vec[
				dict['id' => 1000, 'group_id' => 12345, 'description' => 'desc1'],
				dict['id' => 1001, 'group_id' => 12345, 'description' => 'desc2'],
				dict['id' => 1002, 'group_id' => 12345, 'description' => 'desc3'],
				dict['id' => 1003, 'group_id' => 7, 'description' => 'desc1'],
				dict['id' => 1004, 'group_id' => 7, 'description' => 'desc2'],
			],
			'table5' => vec[
				dict['id' => 1000, 'test_type' => 0x0, 'description' => 'desc0'],
				dict['id' => 1001, 'test_type' => 0x1, 'description' => 'desc1'],
				dict['id' => 1002, 'test_type' => 0x1, 'description' => 'desc2'],
				dict['id' => 1003, 'test_type' => 0x2, 'description' => 'desc3'],
				dict['id' => 1004, 'test_type' => 0x1, 'description' => 'desc4'],
			],
			'association_table' => vec[
				dict['table_3_id' => 1, 'table_4_id' => 1000, 'group_id' => 12345, 'description' => 'association 1'],
				dict['table_3_id' => 1, 'table_4_id' => 1001, 'group_id' => 12345, 'description' => 'association 2'],
				dict['table_3_id' => 2, 'table_4_id' => 1000, 'group_id' => 12345, 'description' => 'association 3'],
				dict['table_3_id' => 3, 'table_4_id' => 1003, 'group_id' => 0, 'description' => 'association 4'],
			],
		];

		$conn->getServer()->databases['db2'] = $database;

		$vitess_conn = null;
		if ($is_vitess_conn) {
			$vitess_conn = await $pool->connect("example2", 2, 'vitess', '', '');

			$vitess_dbs = dict[
				'vt_table1' => vec[
					dict['id' => 1, 'name' => 'Pallettown Chickenstrips'],
					dict['id' => 2, 'name' => 'Brewery Cuttlefish'],
					dict['id' => 3, 'name' => 'Blasphemy Chowderpants'],
					dict['id' => 4, 'name' => 'Benjamin Ampersand'],
				],
				'vt_table2' => vec[
					dict['id' => 11, 'vt_table1_id' => 1, 'description' => 'no'],
					dict['id' => 12, 'vt_table1_id' => 2, 'description' => 'no'],
					dict['id' => 13, 'vt_table1_id' => 3, 'description' => 'no'],
					dict['id' => 14, 'vt_table1_id' => 4, 'description' => 'no'],
				],
			];

			$vitess_conn->getServer()->databases['vitess'] = $vitess_dbs;
			$vitess_conn->getServer()->setConfig(shape(
				'mysql_version' => '5.7',
				'is_vitess' => true,
				'strict_sql_mode' => false,
				'strict_schema_mode' => false,
				'inherit_schema_from' => 'vitess',
			));
		}
		snapshot('setup');
		// if we wanted to create a vitess connection, return that
		return $vitess_conn ?? $conn;
	}
}

const dict<string, dict<string, table_schema>> TEST_SCHEMA = dict[
	'db1' => dict[
		'table1' => shape(
			'name' => 'table1',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'name',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id'],
				),
				shape(
					'name' => 'name_uniq',
					'type' => 'UNIQUE',
					'fields' => keyset['name'],
				),
			],
		),
		'table2' => shape(
			'name' => 'table2',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'table_1_id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'description',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id'],
				),
				shape(
					'name' => 'table_1_id',
					'type' => 'INDEX',
					'fields' => keyset['table_1_id'],
				),
			],
		),
		'table_with_more_fields' => shape(
			'name' => 'table_with_more_fields',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'name',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
				shape(
					'name' => 'nullable_unique',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => true,
					'hack_type' => 'string',
				),
				shape(
					'name' => 'nullable_default',
					'type' => DataType::INT,
					'length' => 20,
					'null' => true,
					'hack_type' => 'int',
					'default' => '1',
				),
				shape(
					'name' => 'not_null_default',
					'type' => DataType::INT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
					'default' => '2',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id', 'name'],
				),
				shape(
					'name' => 'nullable_unique',
					'type' => 'UNIQUE',
					'fields' => keyset['nullable_unique'],
				),
			],
		),
	],
	'db2' => dict[
		'table3' => shape(
			'name' => 'table3',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'group_id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'name',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id'],
				),
				shape(
					'name' => 'name_uniq',
					'type' => 'UNIQUE',
					'fields' => keyset['name'],
				),
				shape(
					'name' => 'group_id',
					'type' => 'INDEX',
					'fields' => keyset['group_id'],
				),
			],
		),
		'table4' => shape(
			'name' => 'table4',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'group_id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'description',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id'],
				),
				shape(
					'name' => 'group_id',
					'type' => 'INDEX',
					'fields' => keyset['group_id'],
				),
			],
		),
		'table5' => shape(
			'name' => 'table5',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'test_type',
					'type' => DataType::INT,
					'length' => 16,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'description',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
			]
		),
		'association_table' => shape(
			'name' => 'association_table',
			'fields' => vec[
				shape(
					'name' => 'table_3_id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'table_4_id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'description',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
				shape(
					'name' => 'group_id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['table_3_id', 'table_4_id'],
				),
				shape(
					'name' => 'table_4_id',
					'type' => 'INDEX',
					'fields' => keyset['table_4_id'],
				),
			],
		),
	],

];

const dict<string, dict<string, table_schema>> VITESS_TEST_SCHEMA = dict[
	'vitess' => dict[
		'vt_table1' => shape(
			'name' => 'vt_table1',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'name',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id'],
				),
				shape(
					'name' => 'name_uniq',
					'type' => 'UNIQUE',
					'fields' => keyset['name'],
				),
			],
			'vitess_sharding' => shape(
				'keyspace' => 'test_keyspace_one',
				'sharding_key' => 'id',
			),
		),
		'vt_table2' => shape(
			'name' => 'vt_table2',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'vt_table1_id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'description',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id'],
				),
				shape(
					'name' => 'table_1_id',
					'type' => 'INDEX',
					'fields' => keyset['vt_table1_id'],
				),
			],
			'vitess_sharding' => shape(
				'keyspace' => 'test_keyspace_one',
				'sharding_key' => 'vt_table1_id',
			),

		),
		'vt_table_with_more_fields' => shape(
			'name' => 'vt_table_with_more_fields',
			'fields' => vec[
				shape(
					'name' => 'id',
					'type' => DataType::BIGINT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
				),
				shape(
					'name' => 'name',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => false,
					'hack_type' => 'string',
				),
				shape(
					'name' => 'nullable_unique',
					'type' => DataType::VARCHAR,
					'length' => 255,
					'null' => true,
					'hack_type' => 'string',
				),
				shape(
					'name' => 'nullable_default',
					'type' => DataType::INT,
					'length' => 20,
					'null' => true,
					'hack_type' => 'int',
					'default' => '1',
				),
				shape(
					'name' => 'not_null_default',
					'type' => DataType::INT,
					'length' => 20,
					'null' => false,
					'hack_type' => 'int',
					'default' => '2',
				),
			],
			'indexes' => vec[
				shape(
					'name' => 'PRIMARY',
					'type' => 'PRIMARY',
					'fields' => keyset['id', 'name'],
				),
				shape(
					'name' => 'nullable_unique',
					'type' => 'UNIQUE',
					'fields' => keyset['nullable_unique'],
				),
			],
			'vitess_sharding' => shape(
				'keyspace' => 'test_keyspace_two',
				'sharding_key' => 'name',
			),

		),
	],
];
