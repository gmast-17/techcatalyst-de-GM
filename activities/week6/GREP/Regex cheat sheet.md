### Regex Cheat Sheet

[**To Practice Regex Python: regex101.com**](https://regex101.com)

#### Basic Concepts

- **Literal Characters**: Matches the exact characters in the pattern.
  - Example: `hello` matches "hello".

#### Metacharacters

- `.` (Dot): Matches any single character except newline.
  - Example: `a.b` matches "aab", "acb", but not "ab".
- `^` (Caret): Matches the start of the string.
  - Example: `^hello` matches "hello" at the start.
- `$` (Dollar): Matches the end of the string.
  - Example: `world$` matches "world" at the end.
- `*` (Asterisk): Matches 0 or more repetitions of the preceding character.
  - Example: `a*` matches "", "a", "aa".
- `+` (Plus): Matches 1 or more repetitions of the preceding character.
  - Example: `a+` matches "a", "aa", but not "".
- `?` (Question Mark): Matches 0 or 1 repetition of the preceding character.
  - Example: `a?` matches "", "a".
- `[]` (Brackets): Matches any one of the characters inside the brackets.
  - Example: `[abc]` matches "a", "b", or "c".
- `\` (Backslash): Escapes a metacharacter.
  - Example: `\.` matches ".".

#### Character Classes

- `\d`: Matches any digit (0-9).
  - Example: `\d` matches "0", "1", ..., "9".
- `\D`: Matches any non-digit.
  - Example: `\D` matches "a", "b", "@", etc.
- `\w`: Matches any alphanumeric character (a-z, A-Z, 0-9, _).
  - Example: `\w` matches "a", "1", "_".
- `\W`: Matches any non-alphanumeric character.
  - Example: `\W` matches "@", "#", " ".
- `\s`: Matches any whitespace character (space, tab, newline).
  - Example: `\s` matches " ", "\t", "\n".
- `\S`: Matches any non-whitespace character.
  - Example: `\S` matches "a", "1", "@", etc.

#### Quantifiers

- `{n}`: Exactly n occurrences.
  - Example: `a{3}` matches "aaa".
- `{n,}` n or more occurrences.
  - Example: `a{2,}` matches "aa", "aaa", etc.
- `{n,m}`: Between n and m occurrences.
  - Example: `a{2,4}` matches "aa", "aaa", "aaaa".

#### Special Sequences

- Grouping and Capturing:
  - `(abc)`: Groups "abc" together.
  - Example: `(ab)+` matches "ab", "abab", etc.
- Alternation:
  - `a|b`: Matches "a" or "b".
  - Example: `a|b` matches "a" or "b".
- Lookahead:
  - `(?=abc)`: Matches a position followed by "abc".
  - Example: `foo(?=bar)` matches "foo" in "foobar".
- Lookbehind:
  - `(?<=abc)`: Matches a position preceded by "abc".
  - Example: `(?<=foo)bar` matches "bar" in "foobar".

#### Anchors

- `^`: Start of string.
  - Example: `^hello` matches "hello" at the start.
- `$`: End of string.
  - Example: `world$` matches "world" at the end.
- `\b`: Word boundary.
  - Example: `\bword\b` matches "word" as a whole word.
- `\B`: Non-word boundary.
  - Example: `\Bword\B` matches "word" within another word.

----

#### Common Patterns

- Email Address:

  ```
  will add after all activies are completed today
  ```

  - Example: Matches "john.doe@example.com".

- Phone Number:

  ```
  will add after all activies are completed today
  ```

  - Example: Matches "123-456-7890".

- Date (YYYY-MM-DD):

  ```
  will add after all activies are completed today
  ```

  - Example: Matches "2024-08-15".

- URL:

  ```
  will add after all activies are completed today
  ```

  - Example: Matches "[http://example.com](http://example.com/)".