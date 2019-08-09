<?hh // strict

namespace Slack\SQLFake;

use type Facebook\HackTest\HackTest;
use function Facebook\FBExpect\expect;

final class GenerateSchemaTest extends HackTest {

	public async function testGenerateSchema(): Awaitable<void> {
		$expected = dict[
			'test' => shape(
				'name' => 'test',
				'fields' => vec[
					shape(
						'name' => 'id',
						'type' => 'VARCHAR',
						'length' => 255,
						'null' => false,
						'hack_type' => 'string',
					),
					shape(
						'name' => 'value',
						'type' => 'VARCHAR',
						'length' => 255,
						'null' => false,
						'hack_type' => 'string',
					),
				],
				'indexes' => vec[
					shape(
						'name' => 'PRIMARY',
						'type' => 'PRIMARY',
						'fields' => dict[
							'id' => 'id',
						],
					),
				],
			),
			'test2' => shape(
				'name' => 'test2',
				'fields' => vec[
					shape(
						'name' => 'id',
						'type' => 'BIGINT',
						'length' => 20,
						'null' => false,
						'hack_type' => 'int',
						'unsigned' => true,
					),
					shape(
						'name' => 'name',
						'type' => 'VARCHAR',
						'length' => 100,
						'null' => false,
						'hack_type' => 'string',
					),
				],
				'indexes' => vec[
					shape(
						'name' => 'PRIMARY',
						'type' => 'PRIMARY',
						'fields' => dict[
							'id' => 'id',
							'name' => 'name',
						],
					),
					shape(
						'name' => 'name',
						'type' => 'INDEX',
						'fields' => dict[
							'name' => 'name',
						],
					),
				],
			),
			'test3' => shape(
				'name' => 'test3',
				'fields' => vec[
					shape(
						'name' => 'id',
						'type' => 'BIGINT',
						'length' => 20,
						'null' => false,
						'hack_type' => 'int',
						'unsigned' => true,
					),
					shape(
						'name' => 'ch',
						'type' => 'CHAR',
						'length' => 64,
						'null' => true,
						'hack_type' => 'string',
					),
					shape(
						'name' => 'deleted',
						'type' => 'TINYINT',
						'length' => 3,
						'null' => false,
						'hack_type' => 'int',
						'default' => '0',
						'unsigned' => true,
					),
					shape(
						'name' => 'name',
						'type' => 'VARCHAR',
						'length' => 100,
						'null' => false,
						'hack_type' => 'string',
					),
				],
				'indexes' => vec[
					shape(
						'name' => 'PRIMARY',
						'type' => 'PRIMARY',
						'fields' => dict[
							'id' => 'id',
						],
					),
					shape(
						'name' => 'name',
						'type' => 'UNIQUE',
						'fields' => dict[
							'name' => 'name',
						],
					),
				],
			),
		];

		$generator = new SchemaGenerator();
		$sql = \file_get_contents(__DIR__.'/fixtures/SchemaExample.sql');
		$schema = $generator->generateFromString($sql);
		expect($schema['test'])->toHaveSameShapeAs($expected['test']);
		expect($schema['test2'])->toHaveSameShapeAs($expected['test2']);
		expect($schema['test3'])->toHaveSameShapeAs($expected['test3']);
	}
}
