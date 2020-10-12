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
 * Modified heavily by maintainers of hack-sql-fake to hacklang & to only support MySQL's JSON path.
 */

namespace Slack\SQLFake\JSONPath;

use namespace HH\Lib\{C, Str, Vec};

type ExplodedPathType = vec<arraykey>;

type ObjectAtPath = shape(
    'object' => mixed,
    'path' => ExplodedPathType,
);

class WrappedResult<T> {
    public function __construct(public T $value) {}
}

/**
 * This is a [JSONPath](http://goessner.net/articles/JsonPath/) like implementation for PHP.
 * It only aims to support JSONPath as implemented/supported by MySQL.
 *
 * Usage
 * =====
 *       // $json can be a string containing json, a `vec`, a `dict`, a PHP object or null.
 *       // If $json is null (or not present) the JSONObject will be empty.
 *       $jsonObject = new JSONObject();
 *       // or
 *       $jsonObject = new JSONObject($json);
 *
 *       // get
 *       $obj->get($jsonPath);
 *
 *       // replace
 *       $obj->replace($jsonPath, $value);
 *
 *       // get the json representation
 *       $str = $obj->getJSON();
 *       echo $str;
 *
 *       // get the hacklang representation
 *       $obj->getValue();
 *
 *
 * SmartGet
 * --------
 *
 * When creating a new instance of JsonObject, you can pass a second parameter to the constructor.
 * This sets the behaviour of the instance to use SmartGet.
 *
 * What SmartGet does is to determine if the given JsonPath branches at some point, if it does it behaves as usual;
 * otherwise, it will directly return the value pointed to by the given path (not a vec containing a single value).
 *
 *      $json = dict[
 *          "a" => dict[
 *              "b" => 3,
 *              "c" => 4
 *          ]
 *      ];
 *      $obj = new JSONObject($json, true);
 *      $obj->get('$.a.b'); // Returns int(3)
 *      $obj->get('$.a.*'); // Returns vec[int(3), int(4)]
 *
 */
class JSONObject {
    // Child regex
    const RE_CHILD_NAME_INCLUDING_WILDCARD = '/^\.(("?)[\w\_\$^\d][\w\-\$]*(\2)|\*)(.*)/u';
    const RE_RECURSIVE_SELECTOR = '/^\*\*([\w\_\$^\d][\w\-\$]*|\*)(.*)/u';

    // Array expressions
    const RE_ARRAY_INDEX = '/^\[(\*|\d+)\](.*)$/';

    // Tokens
    const TOK_ROOT = '$';
    const TOK_SELECTOR_BEGIN = '[';
    const TOK_SELECTOR_END = ']';
    const TOK_ALL = '*';
    const TOK_CHILD_ACCESS_BEGIN = '.';
    const TOK_DOUBLE_ASTERISK = '**';

    private mixed $jsonObject; // This is any valid type in JSON (vec, dict, num, string, null)
    private bool $smartGet = false;
    private bool $hasDiverged = false;

    /**
     * Class constructor.
     * If $json is null the json object contained will be initialized empty.
     *
     * @param mixed $json json
     * @param bool $smartGet enable smart get
     *
     * @return void
     */
    public function __construct(mixed $json = null, bool $smartGet = false) {
        if ($json is string) {
            $this->jsonObject = \json_decode($json, true, 512, \JSON_FB_HACK_ARRAYS);
            if ($json !== 'null' && $this->jsonObject === null) {
                throw new InvalidJSONException('string does not contain a valid JSON object.');
            }
        } else if ($json is vec<_> || $json is dict<_, _> || \is_object($json)) {
            // We encode & decode here to make sure we only have nested dicts/vecs
            $this->jsonObject = \json_decode(\json_encode($json), true, 512, \JSON_FB_HACK_ARRAYS);
        } else {
            throw new InvalidJSONException('value does not encode a JSON object.');
        }
        $this->smartGet = $smartGet;
    }

    /**
     * Returns the value of the JSON object
     *
     *
     * @return array
     */
    public function getValue(): mixed {
        return $this->jsonObject;
    }

    /**
    * Returns the JSON representation of the JSON object
    *
    *
    * @return string
    */
    public function getJSON(): string {
        return \json_encode($this->jsonObject);
    }

    /**
     * Returns an vec containing objects that match the JsonPath.
     *
     * If smartGet was set to true when creating the instance and
     * the JsonPath given does not branch, it will return the value
     * instead of a vec of size 1.
     *
     * @param string $jsonPath jsonPath
     *
     * @return mixed (vec<mixed> | mixed)
     */
    public function get(string $jsonPath): ?WrappedResult<mixed> {
        if (Str\trim($jsonPath) === self::TOK_ROOT) {
            return new WrappedResult($this->jsonObject);
        }

        $this->hasDiverged = false;

        $jsonObject = $this->jsonObject;
        $result = $this->getMatching($jsonObject, $jsonPath);
        if (C\is_empty($result)) {
            return null;
        }

        if ($this->smartGet && !$this->hasDiverged) {
            return new WrappedResult($result[0]['object']);
        }

        return new WrappedResult(Vec\map($result, $r ==> $r['object']));
    }

    /**
     * Replaces the element that results from the $jsonPath query
     * to $value. This method disallows divering (wildcard) paths.
     * It is a no-op if the given path doesn't already exist.
     *
     * This method returns a new JsonObject with the new value.
     *
     * @param string $jsonPath jsonPath
     * @param mixed $value value
     *
     * @return mixed
     */
    public function replace(string $jsonPath, mixed $value): WrappedResult<JSONObject> {
        if (Str\trim($jsonPath) === self::TOK_ROOT) {
            return new WrappedResult(new JSONObject(\json_encode($value)));
        }

        $this->hasDiverged = false;
        $result = $this->getMatching($this->jsonObject, $jsonPath);
        if ($this->hasDiverged) {
            throw new DivergentJSONPathSetException('Cannot set a value using a wildcard JSON path');
        }

        $out = self::setPathsToValue(
            shape('object' => $this->jsonObject, 'path' => vec[]),
            Vec\map($result, $p ==> $p['path']),
            $value,
        );

        return new WrappedResult(new JSONObject(\json_encode($out)));
    }

    private static function pathMatched(vec<ExplodedPathType> $paths, ExplodedPathType $path): bool {
        return C\contains($paths, $path);
    }

    private static function setPathsToValue(ObjectAtPath $object, vec<ExplodedPathType> $paths, mixed $value): mixed {
        $jsonObject = $object['object'];
        $path = $object['path'];

        if ($jsonObject is vec<_> || $jsonObject is dict<_, _>) {
            $out = $jsonObject is vec<_> ? vec[] : dict[];
            foreach ($jsonObject as $key => $original_value) {
                $childPath = Vec\concat($path, vec[$key]);
                $newValue = self::pathMatched($paths, $childPath)
                    ? $value
                    : self::setPathsToValue(shape('object' => $original_value, 'path' => $childPath), $paths, $value);

                if ($out is vec<_>) {
                    $out[] = $newValue;
                } else if ($out is dict<_, _>) {
                    $out[$key] = $newValue;
                }
            }

            return $out;
        }

        return $jsonObject;
    }

    private static function matchArrayIndex(string $jsonPath): ?shape('index' => string, 'rest' => string) {
        if (Str\is_empty($jsonPath) || $jsonPath[0] != self::TOK_SELECTOR_BEGIN) {
            return null;
        }

        $matches = null;
        if (\preg_match_with_matches(self::RE_ARRAY_INDEX, $jsonPath, inout $matches)) {
            return shape(
                'index' => $matches[1],
                'rest' => $matches[2],
            );
        }

        return null;
    }

    private static function matchChildAccess(string $jsonPath): ?shape('child' => string, 'rest' => string) {
        if (Str\is_empty($jsonPath) || $jsonPath[0] != self::TOK_CHILD_ACCESS_BEGIN) {
            return null;
        }

        $matches = null;
        if (\preg_match_with_matches(self::RE_CHILD_NAME_INCLUDING_WILDCARD, $jsonPath, inout $matches)) {
            // Remove double quotedness if matched
            $child = ($matches[2] === '"' && $matches[3] === '"')
                ? Str\strip_prefix($matches[1], '"') |> Str\strip_suffix($$, '"')
                : $matches[1];

            return shape(
                'child' => $child,
                'rest' => $matches[4],
            );
        }

        return null;
    }

    private function opChildName(ObjectAtPath $objectAtPath, string $childName): vec<ObjectAtPath> {
        $out = vec[];

        $jsonObject = $objectAtPath['object'];
        $path = $objectAtPath['path'];

        if ($jsonObject is KeyedContainer<_, _>) {
            if ($childName === self::TOK_ALL) {
                $this->hasDiverged = true;

                foreach ($jsonObject as $key => $item) {
                    // We ignore int indices (varray) here
                    if ($key is int) {
                        continue;
                    }

                    $out[] = shape(
                        'object' => $item,
                        'path' => Vec\concat($path, vec[$key]),
                    );
                }
            } else if (C\contains_key($jsonObject, $childName)) {
                $out[] = shape(
                    'object' => $jsonObject[$childName],
                    'path' => Vec\concat($path, vec[$childName]),
                );
            }
        }

        return $out;
    }

    private function opChildSelector(ObjectAtPath $objectAtPath, string $contents): vec<ObjectAtPath> {
        $jsonObject = $objectAtPath['object'];
        $path = $objectAtPath['path'];

        if ($jsonObject is KeyedContainer<_, _>) {
            if ($contents === self::TOK_ALL) {
                $this->hasDiverged = true;

                $out = vec[];
                foreach ($jsonObject as $key => $item) {
                    $out[] = shape(
                        'object' => $item,
                        'path' => Vec\concat($path, vec[$key]),
                    );
                }
                return $out;
            }

            $index = Str\to_int($contents);
            if ($index is nonnull) {
                if (C\contains_key($jsonObject, $index)) {
                    return vec[shape(
                        'object' => $jsonObject[$index],
                        'path' => Vec\concat($path, vec[$index]),
                    )];
                }

                return vec[];
            }

            throw new InvalidJSONPathException($contents);
        }

        return vec[];
    }

    private static function matchRecursiveSelector(
        string $jsonPath,
    ): ?shape(?'index' => string, ?'child' => string, 'rest' => string) {
        if (!Str\starts_with($jsonPath, self::TOK_DOUBLE_ASTERISK)) {
            return null;
        }

        $jsonPath = Str\strip_prefix($jsonPath, self::TOK_DOUBLE_ASTERISK);

        $matchedArrayIndex = self::matchArrayIndex($jsonPath);
        if ($matchedArrayIndex) {
            return shape('index' => $matchedArrayIndex['index'], 'rest' => $matchedArrayIndex['rest']);
        }

        $matchedChildAccess = self::matchChildAccess($jsonPath);
        if ($matchedChildAccess) {
            return shape('child' => $matchedChildAccess['child'], 'rest' => $matchedChildAccess['rest']);
        }

        return null;
    }

    private function opRecursiveSelector(
        ObjectAtPath $objectAtPath,
        shape(?'index' => string, ?'child' => string, 'rest' => string) $matched,
    ): vec<ObjectAtPath> {
        $this->hasDiverged = true;

        $out = vec[];

        $jsonObject = $objectAtPath['object'];
        $path = $objectAtPath['path'];

        $childName = $matched['child'] ?? null;
        if ($childName is nonnull) {
            $ret = $this->opChildName($objectAtPath, $childName);
            $out = Vec\concat($out, $ret);

            if ($jsonObject is KeyedContainer<_, _>) {
                foreach ($jsonObject as $key => $value) {
                    $ret = $this->opRecursiveSelector(
                        shape('object' => $value, 'path' => Vec\concat($path, vec[$key])),
                        $matched,
                    );
                    $out = Vec\concat($out, $ret);
                }
            }
        }

        $index = $matched['index'] ?? null;
        if ($index is nonnull) {
            $ret = $this->opChildSelector($objectAtPath, $index);
            $out = Vec\concat($out, $ret);

            if ($jsonObject is KeyedContainer<_, _>) {
                foreach ($jsonObject as $key => $value) {
                    $ret = $this->opRecursiveSelector(
                        shape('object' => $value, 'path' => Vec\concat($path, vec[$key])),
                        $matched,
                    );
                    $out = Vec\concat($out, $ret);
                }
            }
        }

        return $out;
    }

    private function getMatching(mixed $jsonObject, string $jsonPath): vec<ObjectAtPath> {
        if (!Str\starts_with($jsonPath, self::TOK_ROOT)) {
            throw new InvalidJSONPathException($jsonPath);
        }

        $jsonPath = Str\strip_prefix($jsonPath, self::TOK_ROOT);
        $selection = vec[shape('object' => $jsonObject, 'path' => vec[])];
        while (!Str\is_empty($jsonPath) && !C\is_empty($selection)) {
            $newSelection = vec[];

            $matchedChildAccess = self::matchChildAccess($jsonPath);
            if ($matchedChildAccess is nonnull) {
                foreach ($selection as $jsonObject) {
                    $ret = $this->opChildName($jsonObject, $matchedChildAccess['child']);
                    $newSelection = Vec\concat($newSelection, $ret);
                }

                if (C\is_empty($newSelection)) {
                    $selection = vec[];
                    break;
                } else {
                    $jsonPath = $matchedChildAccess['rest'];
                }

                $selection = $newSelection;
                continue;
            }

            $matchedArrayIndex = self::matchArrayIndex($jsonPath);
            if ($matchedArrayIndex) {
                $index = $matchedArrayIndex['index'];
                foreach ($selection as $jsonObject) {
                    $ret = $this->opChildSelector($jsonObject, $index);
                    $newSelection = Vec\concat($newSelection, $ret);
                }

                if (C\is_empty($newSelection)) {
                    $selection = vec[];
                    break;
                } else {
                    $jsonPath = $matchedArrayIndex['rest'];
                }

                $selection = $newSelection;
                continue;
            }

            $matchedRecursive = self::matchRecursiveSelector($jsonPath);
            if ($matchedRecursive) {
                $this->hasDiverged = true;
                foreach ($selection as $jsonObject) {
                    $ret = $this->opRecursiveSelector($jsonObject, $matchedRecursive);
                    $newSelection = Vec\concat($newSelection, $ret);
                }

                if (C\is_empty($newSelection)) {
                    $selection = vec[];
                    break;
                } else {
                    $jsonPath = $matchedRecursive['rest'];
                }

                $selection = $newSelection;
                continue;
            }

            throw new InvalidJSONPathException($jsonPath);
        }

        return $selection;
    }
}
