<?hh // strict

namespace Slack\SQLFake;

use type Facebook\HackTest\HackTest;
use function Facebook\FBExpect\expect;

final class GenerateSchemaTest extends HackTest {

	public async function testGenerateSchema(): Awaitable<void> {
		$expected = dict[
			'test' => new TableSchema(
				'test',
				vec[
					new Column('id', DataType::VARCHAR, 255, false, 'string'),
					new Column('value', DataType::VARCHAR, 255, false, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id']),
				],
			),
			'test2' => new TableSchema(
				'test2',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int', true),
					new Column('name', DataType::VARCHAR, 100, false, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id', 'name']),
					new Index('name', 'INDEX', keyset['name']),
				],
			),
			'test3' => new TableSchema(
				'test3',
				vec[
					new Column('id', DataType::BIGINT, 20, false, 'int', true),
					new Column('ch', DataType::CHAR, 64, true, 'string'),
					new Column('deleted', DataType::TINYINT, 3, false, 'int', true, '0'),
					new Column('name', DataType::VARCHAR, 100, false, 'string'),
				],
				vec[
					new Index('PRIMARY', 'PRIMARY', keyset['id']),
					new Index('name', 'UNIQUE', keyset['name']),
				],
			),
		];

		$generator = new SchemaGenerator();
		$sql = \file_get_contents(__DIR__.'/fixtures/SchemaExample.sql');
		$schema = $generator->generateFromString($sql);
		expect(\var_export($schema['test'], true))->toEqual(\var_export($expected['test'], true));
		expect(\var_export($schema['test2'], true))->toEqual(\var_export($expected['test2'], true));
		expect(\var_export($schema['test3'], true))->toEqual(\var_export($expected['test3'], true));
		$expected_schema = \file_get_contents(__DIR__.'/fixtures/expected_schema.codegen');
		$string_schema =
			BuildSchemaCLI::getRenderedHackTableSchemaWithClusters('get_my_table_schemas', dict['prod' => $schema]);
		expect($string_schema)->toBeSame($expected_schema);
	}
}
