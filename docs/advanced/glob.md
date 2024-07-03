Glob Pattern
=============

The glob module finds all the pathnames matching a specified pattern according to the rules.

### Patterns are Unix shell style:

|pattern        |meaning                       |
|---------------|------------------------------|
|`*`            |matches any characters but '/'|
|`**`           |matches everything            |
|`?`            |matches any single character  |
|`[seq]`        |matches any character in seq  |
|`[!seq]`       |matches any char not in seq   |
|`{seq1,seq2}`  |matches seq1 or seq2          |