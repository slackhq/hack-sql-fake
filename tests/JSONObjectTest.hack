/**
 * Copyright 2018 Alessio Linares
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Based on code from https://github.com/Galbar/JsonPath-PHP
 *
 * Modified by maintainers of hack-sql-fake as needed
 */

namespace Slack\SQLFake\JSONPath;

use function Facebook\FBExpect\expect;
use type Facebook\HackTest\{DataProvider, HackTest};

use type Slack\SQLFake\JSONPath\{JSONException, JSONObject};

type TestCase = shape(
  'result' => mixed,
  'path' => string,
);

final class JSONObjectTest extends HackTest {
  private string $json = '
    {
      "store": {
        "book": [
          {
            "category": "reference",
            "author": "Nigel Rees",
            "title": "Sayings of the Century",
            "price": 8.95,
            "available": true
          },
          {
            "category": "fiction",
            "author": "Evelyn Waugh",
            "title": "Sword of Honour",
            "price": 12.99,
            "available": false
          },
          {
            "category": "fiction",
            "author": "Herman Melville",
            "title": "Moby Dick",
            "isbn": "0-553-21311-3",
            "price": 8.99,
            "available": true
          },
          {
            "category": "fiction",
            "author": "J. R. R. Tolkien",
            "title": "The Lord of the Rings",
            "isbn": "0-395-19395-8",
            "price": 22.99,
            "available": false
          }
        ],
        "bicycle": {
          "color": "red",
          "price": 19.95,
          "available": true,
          "model": null,
          "sku-number": "BCCLE-0001-RD"
        }
      },
      "authors": [
        "Nigel Rees",
        "Evelyn Waugh",
        "Herman Melville",
        "J. R. R. Tolkien"
      ],
      "Bike models": [
        1,
        2,
        3
      ],
      "movies": [
        {
          "name": "Movie 1",
          "director": "Director 1"
        }
      ],
      "$under_$-score3d": 2
    }
  ';

  public static async function testGetProvider(): Awaitable<vec<mixed>> {
    return vec[
      tuple('$.store.book[-4, -2, -1]', shape('exceptional' => true)),
      tuple('$.store.bicycle.price', shape('value' => vec[19.95])),
      tuple('$."store".bicycle."price"', shape('value' => vec[19.95])),
      tuple('$."store".bicycle.model', shape('value' => vec[null])),
      tuple('$.store.bicycle.sku-number', shape('value' => vec['BCCLE-0001-RD'])),
      tuple('$.store.bicycle."sku-number"', shape('value' => vec['BCCLE-0001-RD'])),
      tuple('$.$under_$-score3d', shape('value' => vec[2])),
      tuple('$."$under_$-score3d"', shape('value' => vec[2])),
      tuple('$.-$under_$-score3d', shape('exceptional' => true)),
      tuple('$.0$under_$-score3d', shape('exceptional' => true)),
      tuple(
        '$.store.bicycle',
        shape(
          'value' => vec[
            dict[
              'color' => 'red',
              'price' => 19.95,
              'available' => true,
              'model' => null,
              'sku-number' => 'BCCLE-0001-RD',
            ],
          ],
        ),
      ),
      tuple('$.store.bicycl', shape('value' => null)),
      tuple('$.store.book[*].price', shape('value' => vec[8.95, 12.99, 8.99, 22.99])),
      tuple('$.store.book[0][price]', shape('exceptional' => true)),
      tuple('$.store.book[0]["price"]', shape('exceptional' => true)),
      tuple('$.store.book.0', shape('exceptional' => true)),
      tuple('$.store.book[7]', shape('value' => null)),
      tuple('$.store.book[1, 2].price', shape('exceptional' => true)),
      tuple('$.store.book[*][category, author]', shape('exceptional' => true)),
      tuple("$.store.book[*]['category', \"author\"]", shape('exceptional' => true)),
      tuple('$.store.book[0:3:2].price', shape('exceptional' => true)),
      tuple('$.store.bicycle.price[2]', shape('value' => null)),
      tuple('$.store.bicycle.price.*', shape('value' => null)),
      tuple(
        '$.store.bicycle.*',
        shape(
          'value' => vec[
            'red',
            19.95,
            true,
            null,
            'BCCLE-0001-RD',
          ],
        ),
      ),
      tuple('$**.price', shape('value' => vec[8.95, 12.99, 8.99, 22.99, 19.95])),
      tuple('$**."price"', shape('value' => vec[8.95, 12.99, 8.99, 22.99, 19.95])),
      tuple('$**price', shape('exceptional' => true)),
      tuple('$***.price', shape('exceptional' => true)),
      tuple("$.store.book[?(@.category == 'fiction')].price", shape('exceptional' => true)),
      tuple('$**[?(@.available == true)].price', shape('exceptional' => true)),
      tuple('$**[?(@.available == false)].price', shape('exceptional' => true)),
      tuple('$**[?(@.price < 10)].title', shape('exceptional' => true)),
      tuple('$**[?(@.price < 10.0)].title', shape('exceptional' => true)),
      tuple('$.store.book[?(@.price > 10)].title', shape('exceptional' => true)),
      tuple('$**[?(@.author =~ /.*Tolkien/)].title', shape('exceptional' => true)),
      tuple('$**[?(@.length <= 5)].color', shape('exceptional' => true)),
      tuple('$**[?(@.length <= 5.0)].color', shape('exceptional' => true)),
      tuple('$.store.book[?(@.author == $.authors[3])].title', shape('exceptional' => true)),
      tuple('$**[?(@.price >= 19.95)][author, color]', shape('exceptional' => true)),
      tuple(
        "$**[?(@.category == 'fiction' and @.price < 10 or @.color == \"red\")].price",
        shape('exceptional' => true),
      ),
      tuple("$.store.book[?(not @.category == 'fiction')].price", shape('exceptional' => true)),
      tuple("$.store.book[?(@.category != 'fiction')].price", shape('exceptional' => true)),
      tuple('$**[?(@.color)].color', shape('exceptional' => true)),
      tuple("$.store[?(not @..price or @..color == 'red')].available", shape('exceptional' => true)),
      tuple('$.store[?(@.price.length == 3)]', shape('exceptional' => true)),
      tuple('$.store[?(@.color.length == 3)].price', shape('exceptional' => true)),
      tuple('$.store[?(@.color.length == 5)].price', shape('exceptional' => true)),
      tuple('$.store[?(@.*.length == 3)]', shape('exceptional' => true)),
      tuple('$.store..*[?(@..model == null)].color', shape('exceptional' => true)),
      tuple("$['Bike models']", shape('exceptional' => true)),
      tuple('$["Bike models"]', shape('exceptional' => true)),
      tuple(
        '$**[1]',
        shape(
          'value' => vec[
            dict[
              'category' => 'fiction',
              'author' => 'Evelyn Waugh',
              'title' => 'Sword of Honour',
              'price' => 12.99,
              'available' => false,
            ],
            'Evelyn Waugh',
            2,
          ],
        ),
      ),
      tuple(
        '$.movies**.*',
        shape(
          'value' => vec[
            'Movie 1',
            'Director 1',
          ],
        ),
      ),
    ];
  }

  <<DataProvider('testGetProvider')>>
  public async function testGet(
    string $jsonPath,
    shape(?'exceptional' => bool, ?'value' => mixed) $output,
  ): Awaitable<void> {
    $exceptional = $output['exceptional'] ?? false;

    $jsonObject = new JSONObject($this->json);
    if (!$exceptional) {
      invariant(Shapes::keyExists($output, 'value'), 'expected value must be present in non-exceptional cases');

      $value = $output['value'];
      $results = $jsonObject->get($jsonPath);


      if ($value is nonnull) {
        expect($results)->toNotBeNull();
        invariant($results is nonnull, 'just making typechecker happy');
        expect($results->value)->toEqual($output['value'], $jsonPath);
      } else {
        expect($results)->toBeNull();
      }

      return;
    }

    expect(() ==> $jsonObject->get($jsonPath))->toThrow(JSONException::class);
  }

  public static async function testSmartGetProvider(): Awaitable<vec<mixed>> {
    return vec[
      tuple('$.store.bicycle.price', shape('value' => 19.95)),
      tuple('$."store".bicycle."price"', shape('value' => 19.95)),
      tuple(
        '$.store.bicycle',
        shape(
          'value' => dict[
            'color' => 'red',
            'price' => 19.95,
            'available' => true,
            'model' => null,
            'sku-number' => 'BCCLE-0001-RD',
          ],
        ),
      ),
      tuple(
        '$**.bicycle',
        shape(
          'value' => vec[
            dict[
              'color' => 'red',
              'price' => 19.95,
              'available' => true,
              'model' => null,
              'sku-number' => 'BCCLE-0001-RD',
            ],
          ],
        ),
      ),
      tuple('$**.sku-number', shape('value' => vec['BCCLE-0001-RD'])),
      tuple('$.store.bicycl', shape('value' => null)),
      tuple('$.store.book[*].price', shape('value' => vec[8.95, 12.99, 8.99, 22.99])),
      tuple('$.store.book[7]', shape('value' => null)),
      tuple('$.store.bicycle.*', shape('value' => vec['red', 19.95, true, null, 'BCCLE-0001-RD'])),
      tuple('$**.price', shape('value' => vec[8.95, 12.99, 8.99, 22.99, 19.95])),
    ];
  }

  <<DataProvider('testSmartGetProvider')>>
  public async function testSmartGet(
    string $jsonPath,
    shape(?'exceptional' => bool, ?'value' => mixed) $output,
  ): Awaitable<void> {
    $exceptional = $output['exceptional'] ?? false;

    $jsonObject = new JSONObject($this->json, true);
    if (!$exceptional) {
      invariant(Shapes::keyExists($output, 'value'), 'expected value must be present in non-exceptional cases');

      $value = $output['value'];
      $results = $jsonObject->get($jsonPath);

      if ($value is nonnull) {
        expect($results)->toNotBeNull();
        invariant($results is nonnull, 'just making typechecker happy');
        expect($results->value)->toEqual($output['value'], $jsonPath);
      } else {
        expect($results)->toBeNull();
      }

      return;
    }

    expect(() ==> $jsonObject->get($jsonPath))->toThrow(JSONException::class);
  }

  public static async function testReplaceProvider(): Awaitable<vec<mixed>> {
    return vec[
      tuple(
        shape(
          'json' => dict[
            'bicycle' => dict[
              'price' => 19.95,
              'color' => 'red',
              'sku-number' => 'BCCLE-0001-RD',
            ],
          ]
            |> \json_encode($$),
          'path' => '$.bicycle.price',
          'value' => 2000.01,
        ),
        shape(
          'value' => dict[
            'bicycle' => dict[
              'price' => 2000.01,
              'color' => 'red',
              'sku-number' => 'BCCLE-0001-RD',
            ],
          ],
        ),
      ),
      tuple(
        shape(
          'json' => dict[
            'bicycle' => vec[
              dict[
                'price' => 19.95,
                'color' => 'red',
                'sku-number' => 'BCCLE-0001-RD',
              ],
              dict[
                'price' => 19.96,
                'color' => 'blue',
                'sku-number' => 'BCCLE-0002-RD',
              ],
            ],
          ]
            |> \json_encode($$),
          'path' => '$.bicycle[1].price',
          'value' => 2000.01,
        ),
        shape(
          'value' => dict[
            'bicycle' => vec[
              dict[
                'price' => 19.95,
                'color' => 'red',
                'sku-number' => 'BCCLE-0001-RD',
              ],
              dict[
                'price' => 2000.01,
                'color' => 'blue',
                'sku-number' => 'BCCLE-0002-RD',
              ],
            ],
          ],
        ),
      ),
      tuple(
        shape(
          'json' => dict[
            'bicycle' => dict[
              'price' => 19.95,
              'color' => 'red',
              'sku-number' => 'BCCLE-0001-RD',
            ],
          ],
          'path' => '$.bicycle[*]',
          'value' => 2000,
        ),
        shape('exceptional' => true),
      ),
      tuple(
        shape(
          'json' => dict[
            'bicycle' => dict[
              'price' => 19.95,
              'color' => 'red',
              'sku-number' => 'BCCLE-0001-RD',
            ],
          ],
          'path' => '$**.color',
          'value' => 'blue',
        ),
        shape('exceptional' => true),
      ),
      tuple(
        shape(
          'json' => dict[
            'bicycle' => vec[
              dict[
                'price' => 19.95,
                'color' => 'red',
                'sku-number' => 'BCCLE-0001-RD',
              ],
              dict[
                'price' => 19.96,
                'color' => 'blue',
                'sku-number' => 'BCCLE-0002-RD',
              ],
            ],
          ]
            |> \json_encode($$),
          'path' => '$.bicycle[0]',
          'value' => true,
        ),
        shape(
          'value' => dict[
            'bicycle' => vec[
              true,
              dict[
                'price' => 19.96,
                'color' => 'blue',
                'sku-number' => 'BCCLE-0002-RD',
              ],
            ],
          ],
        ),
      ),
      tuple(shape('json' => '2', 'path' => '$', 'value' => 3), shape('value' => 3)),
    ];
  }

  <<DataProvider('testReplaceProvider')>>
  public async function testReplace(
    shape('json' => mixed, 'path' => string, 'value' => mixed) $input,
    shape(?'exceptional' => bool, ?'value' => mixed) $output,
  ): Awaitable<void> {
    $jsonPath = $input['path'];
    $value = $input['value'];

    $exceptional = $output['exceptional'] ?? false;

    $jsonObject = new JSONObject($input['json']);
    if (!$exceptional) {
      invariant(Shapes::keyExists($output, 'value'), 'expected value must be present in non-exceptional cases');
      $result = $jsonObject->replace($jsonPath, $value);
      expect($result->value->getValue())->toEqual($output['value'], $jsonPath);
      return;
    }

    expect(() ==> $jsonObject->replace($jsonPath, $value))->toThrow(JSONException::class);
  }

  public static async function testConstructorErrorsProvider(): Awaitable<vec<mixed>> {
    return vec[
      tuple(5),
      tuple('{"invalid": json}'),
      tuple(darray[]),
      tuple(varray[]),
    ];
  }

  <<DataProvider('testConstructorErrorsProvider')>>
  public async function testConstructErrors(mixed $json): Awaitable<void> {
    expect(() ==> new JSONObject($json))->toThrow(JSONException::class);
  }
}
