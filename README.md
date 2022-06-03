# Database System Project

Project for the Database System class in 2022-1

## Introduction

Simple DBMS package supports following features

* Table create / delete
* Record create / read / delete

Update is not supported in both table and record

### Available column types

* CHAR (up to 255 bytes)
* VARCHAR (up to 255 bytes)

All columns except primary column can be either not null or nullable

### Supported WHERE operations

* =
* !=
* IS NULL
* IS NOT NULL

`and` and `or` operations are not supported

Selecting specific column is not supported (MUST fetch all columns in the table)

## Getting started

### compile

```shell
javac -cp bin -d bin src/**/*.java test/IntegrationTest.java
```

### run integration test

```shell
java -cp bin IntegrationTest
```

## Integration test result

```text
SELECT * FROM student
+-------+----------+------------+----------+
| ID    | name     | dept_name  | tot_cred |
+-------+----------+------------+----------+
| 00128 | Zhang    | Comp. Sci. | 102      |
| 12345 | Shankar  | Comp. Sci. | 32       |
| 19991 | Brandt   | History    | 80       |
| 23121 | Chavez   | Finance    | 110      |
| 44553 | Peltier  | Physics    | 56       |
| 45678 | Levy     | Physics    | 46       |
| 54321 | Williams | Comp. Sci. | 54       |
| 55739 | Sanchez  | Music      | 38       |
| 70557 | Snow     | Physics    | 0        |
| 76543 | Brown    | Comp. Sci. | 58       |
| 76653 | Aoi      | Elec. Eng. | 60       |
| 98765 | Bourikas | Elec. Eng. | 98       |
| 98988 | Tanaka   | Biology    | 120      |
+-------+----------+------------+----------+

SELECT * FROM student WHERE name = 'Brown'
+-------+-------+------------+----------+
| ID    | name  | dept_name  | tot_cred |
+-------+-------+------------+----------+
| 76543 | Brown | Comp. Sci. | 58       |
+-------+-------+------------+----------+

SELECT * FROM student WHERE dept_name != 'Comp. Sci.'
+-------+----------+------------+----------+
| ID    | name     | dept_name  | tot_cred |
+-------+----------+------------+----------+
| 19991 | Brandt   | History    | 80       |
| 23121 | Chavez   | Finance    | 110      |
| 44553 | Peltier  | Physics    | 56       |
| 45678 | Levy     | Physics    | 46       |
| 55739 | Sanchez  | Music      | 38       |
| 70557 | Snow     | Physics    | 0        |
| 76653 | Aoi      | Elec. Eng. | 60       |
| 98765 | Bourikas | Elec. Eng. | 98       |
| 98988 | Tanaka   | Biology    | 120      |
+-------+----------+------------+----------+

Delete all students in which dept_name = 'Elec. Eng.'
SELECT * FROM student
+-------+----------+------------+----------+
| ID    | name     | dept_name  | tot_cred |
+-------+----------+------------+----------+
| 00128 | Zhang    | Comp. Sci. | 102      |
| 12345 | Shankar  | Comp. Sci. | 32       |
| 19991 | Brandt   | History    | 80       |
| 23121 | Chavez   | Finance    | 110      |
| 44553 | Peltier  | Physics    | 56       |
| 45678 | Levy     | Physics    | 46       |
| 54321 | Williams | Comp. Sci. | 54       |
| 55739 | Sanchez  | Music      | 38       |
| 70557 | Snow     | Physics    | 0        |
| 76543 | Brown    | Comp. Sci. | 58       |
| 98988 | Tanaka   | Biology    | 120      |
+-------+----------+------------+----------+

Delete all students in which dept_name != 'Comp. Sci.'
SELECT * FROM student
+-------+----------+------------+----------+
| ID    | name     | dept_name  | tot_cred |
+-------+----------+------------+----------+
| 00128 | Zhang    | Comp. Sci. | 102      |
| 12345 | Shankar  | Comp. Sci. | 32       |
| 54321 | Williams | Comp. Sci. | 54       |
| 76543 | Brown    | Comp. Sci. | 58       |
+-------+----------+------------+----------+

Delete all student records
SELECT * FROM student
+----+------+-----------+----------+
| ID | name | dept_name | tot_cred |
+----+------+-----------+----------+
+----+------+-----------+----------+

Delete student table
SELECT * FROM student
Table [student] does not exists

SELECT * FROM department
+------------+----------+--------+
| dept_name  | building | budget |
+------------+----------+--------+
| Biology    | Watson   | 90000  |
| Comp. Sci. | Taylor   | 100000 |
| Elec. Eng. | Taylor   | 85000  |
| Finance    | Painter  | 120000 |
| History    | Painter  | 50000  |
| Music      | Packard  | 80000  |
| Physics    | Watson   | 70000  |
| Test       | 310      | (null) |
+------------+----------+--------+

SELECT * FROM department WHERE dept_name = 'Comp. Sci.'
+------------+----------+--------+
| dept_name  | building | budget |
+------------+----------+--------+
| Comp. Sci. | Taylor   | 100000 |
+------------+----------+--------+

SELECT * FROM department WHERE budget IS NULL
+-----------+----------+--------+
| dept_name | building | budget |
+-----------+----------+--------+
| Test      | 310      | (null) |
+-----------+----------+--------+

SELECT * FROM department WHERE budget IS NOT NULL
+------------+----------+--------+
| dept_name  | building | budget |
+------------+----------+--------+
| Biology    | Watson   | 90000  |
| Comp. Sci. | Taylor   | 100000 |
| Elec. Eng. | Taylor   | 85000  |
| Finance    | Painter  | 120000 |
| History    | Painter  | 50000  |
| Music      | Packard  | 80000  |
| Physics    | Watson   | 70000  |
+------------+----------+--------+

Delete all departments in which budget is null
SELECT * FROM department
+------------+----------+--------+
| dept_name  | building | budget |
+------------+----------+--------+
| Biology    | Watson   | 90000  |
| Comp. Sci. | Taylor   | 100000 |
| Elec. Eng. | Taylor   | 85000  |
| Finance    | Painter  | 120000 |
| History    | Painter  | 50000  |
| Music      | Packard  | 80000  |
| Physics    | Watson   | 70000  |
+------------+----------+--------+

Delete all departments in which budget is not null
SELECT * FROM department
+-----------+----------+--------+
| dept_name | building | budget |
+-----------+----------+--------+
+-----------+----------+--------+
```
