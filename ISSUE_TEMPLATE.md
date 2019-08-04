# Issue template for hack-sql-fake

### Please select the options that apply

 - [ ] Feature request
 - [ ] Question
 - [ ] Suggestion
 - [ ] Typechecker errors
 - [ ] Incorrect result
 - [ ] Cripplingly bad performance
 - [ ] Crash
 
### Follow the template for your checked checkbox and then delete the templates.
 
#### Feature request

```
I would like to have ___ implemented.
This is an SQL/AsyncMysql feature.
It should behave like this:
(Either link to the SQL documentation or describe the behavior)
```

#### Suggestion

```
Free form
```

#### Question

```
Free form
```

#### Typechecker errors

```
On HHVM version
----------------

Output from hhvm --version
Output from hh_client --version

----------------

I get this typechecker error
----------------

Output from hh_client

----------------

I can tell the typecheck is right/mistaken because,
----------------

Either an explanation of why this is typesafe at runtime or a case in which this could go wrong

----------------

I think you can fix it by (open a PR) 😉
```

#### Incorrect result

```
The following example:
----------------

<?hh // strict
// Minimal example code here

----------------

has this result
----------------

// result here

----------------

but I expected
----------------

// expected result

----------------
```

#### Cripplingly bad performance
```
This query
----------------

Query here

----------------

takes X seconds with a dataset of size X rows
```

#### Crash
```
The following code:
----------------

<?hh // strict
// Minimal example code here

----------------

crashes on HHVM version
----------------

hhvm --version output here

----------------

with the following stacetrace
----------------

Stacktrace here

----------------
```
