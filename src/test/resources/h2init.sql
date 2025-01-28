-- H2 database preparation (schemas and test data)
--
-- Copyright 2024 Karl Eilebrecht
-- 
-- Licensed under the Apache License, Version 2.0 (the "License"):
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
-- http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

CREATE SCHEMA IF NOT EXISTS adl;
SET SCHEMA adl;

DROP TABLE T_BASE IF EXISTS;
DROP TABLE T_FACTS IF EXISTS;
DROP TABLE T_SURVEY IF EXISTS;
DROP TABLE T_POSDATA IF EXISTS;
DROP TABLE T_FLAT_TXT IF EXISTS;

-- table that contains all IDs, and there is only one row per ID
CREATE TABLE T_BASE (
	ID INT PRIMARY KEY,
	PROVIDER VARCHAR(255) NOT NULL,
	COUNTRY VARCHAR(255),
	CITY VARCHAR(255),
	DEM_CODE INT,
	GENDER VARCHAR(20),
	OM_SCORE FLOAT,
	UPD_TIME TIMESTAMP,
	UPD_DATE DATE,
	TNT_CODE TINYINT,
	B_STATE BOOLEAN,
	S_CODE SMALLINT,
	BI_CODE BIGINT,
	N_CODE NUMERIC(20,2)
);

INSERT INTO T_BASE (ID, PROVIDER, COUNTRY, CITY, DEM_CODE, GENDER, OM_SCORE, UPD_TIME, UPD_DATE, TNT_CODE, B_STATE, S_CODE, BI_CODE, N_CODE) 
VALUES
(19011, 'LOGMOTH', 'USA', NULL, 7, NULL, 1723.9, TIMESTAMP '2024-09-24 17:38:21', DATE '2024-09-24', 2, TRUE, 89, 198234523, NULL),
(19012, 'LOGMOTH', NULL, NULL, 9, 'male', 8765.9, TIMESTAMP '2024-09-25 11:38:21', DATE '2024-09-25', NULL, NULL, NULL, NULL, 2031122.13),
(19013, 'LOGMOTH', 'USA', 'Denver', 8, 'female', 1265.3, TIMESTAMP '2024-09-24 11:38:21', DATE '2024-09-24', 3, FALSE, 34, 18726323, 5631124.49),
(19014, 'LOGMOTH', 'GERMANY', NULL, 8, 'female', 265.3, TIMESTAMP '2024-09-25 20:38:00', DATE '2024-09-25', 3, TRUE, 17, NULL, NULL),
(19015, 'LOGMOTH', 'UK', 'London', 9, 'male', 8265.3, TIMESTAMP '2024-09-27 19:38:01', DATE '2024-09-27', 5, NULL, NULL, 8812731, 19273213.21),
(19016, 'LOGMOTH', 'UK', 'London', 9, 'diverse', 3214.9, TIMESTAMP '2024-09-27 19:38:01', DATE '2024-09-27', 2, FALSE, 11, 8812731, 19273213.21),
(19017, 'ZOMBEE', 'FRANCE', NULL, 11, 'female', 1114.5, TIMESTAMP '2024-09-30 14:34:09', DATE '2024-09-30', 6, TRUE, 17, 6612732, 73213.99),
(19018, 'ZOMBEE', 'FRANCE', 'Paris', 11, 'male', 55514.5, TIMESTAMP '2024-09-30 14:34:01', DATE '2024-09-30', 6, TRUE, 11, 5512831, 23732131.87),
(19019, 'CLCPRO', 'USA', 'Chicago', 7, NULL, 5723.6, TIMESTAMP '2024-06-11 13:38:20', DATE '2024-06-11', 2, NULL, NULL, NULL, NULL),
(19020, 'CLCPRO', 'USA', 'Chicago', 5, 'male', 6623.4, TIMESTAMP '2024-06-11 13:50:01', DATE '2024-06-11', 2, NULL, NULL, NULL, NULL),
(19021, 'CLCPRO', 'USA', 'Chicago', 7, NULL, 5723.6, TIMESTAMP '2024-06-11 13:50:20', DATE '2024-06-11', 2, NULL, NULL, NULL, NULL);

-- table that contains some fictional facts about the individuals in T_BASE
CREATE TABLE T_FACTS (
	UID INT NOT NULL,
	PROVIDER VARCHAR(255) NOT NULL,
	F_KEY VARCHAR(20) NOT NULL,
	F_TYPE VARCHAR(20) NOT NULL,
	F_VALUE_STR VARCHAR(255),
	F_VALUE_INT BIGINT,
	F_VALUE_DEC DECIMAL(10,7),
	F_VALUE_FLG BOOLEAN,
	F_VALUE_DT DATE,
	F_VALUE_TS TIMESTAMP 
);

INSERT INTO T_FACTS(UID, PROVIDER, F_KEY, F_TYPE, F_VALUE_STR, F_VALUE_INT, F_VALUE_DEC, F_VALUE_FLG, F_VALUE_DT, F_VALUE_TS)
VALUES
(19011, 'CLCPRO', 'hasPet', 'flag', NULL, NULL, NULL, TRUE, NULL, NULL),
(19011, 'CLCPRO', 'hasDog', 'flag', NULL, NULL, NULL, TRUE, NULL, NULL),
(19011, 'CLCPRO', 'hasCat', 'flag', NULL, NULL, NULL, FALSE, NULL, NULL),
(19011, 'CLCPRO', 'hasBird', 'flag', NULL, NULL, NULL, FALSE, NULL, NULL),
(19011, 'CLCPRO', 'dateOfBirth', 'date', NULL, NULL, NULL, NULL, DATE '2000-03-05', NULL),
(19011, 'CLCPRO', 'yearOfBirth', 'int', NULL, 2000, NULL, NULL, NULL, NULL),
(19011, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2023-12-24 19:36:12'),
(19011, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-02-28 17:11:25'),
(19011, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-07-21 16:31:03'),
(19011, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-07-21 23:01:17'),
(19011, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-12-19 10:18:19'),
(19011, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-12-22 02:48:07'),
(19011, 'CLCPRO', 'contactCode', 'string', 'RX89', NULL, NULL, NULL, NULL, NULL),
(19011, 'CLCPRO', 'xScore', 'decimal', NULL, NULL, 0.9871621, NULL, NULL, NULL),
(19012, 'CLCPRO', 'hasPet', 'flag', NULL, NULL, NULL, TRUE, NULL, NULL),
(19012, 'CLCPRO', 'hasDog', 'flag', NULL, NULL, NULL, FALSE, NULL, NULL),
(19012, 'CLCPRO', 'yearOfBirth', 'int', NULL, 2000, NULL, NULL, NULL, NULL),
(19012, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-03-29 12:15:21'),
(19012, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-08-21 19:32:13'),
(19012, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-08-21 22:11:14'),
(19012, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-12-22 10:18:16'),
(19012, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-12-22 12:48:12'),
(19012, 'CLCPRO', 'contactCode', 'string', 'RX11', NULL, NULL, NULL, NULL, NULL),
(19012, 'CLCPRO', 'xScore', 'decimal', NULL, NULL, 0.621761235, NULL, NULL, NULL),
(19013, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-12-21 23:41:06'),
(19013, 'CLCPRO', 'contactCode', 'string', 'RX89', NULL, NULL, NULL, NULL, NULL),
(19013, 'CLCPRO', 'xScore', 'decimal', NULL, NULL, 0.1871623, NULL, NULL, NULL),
(19014, 'CLCPRO', 'hasPet', 'flag', NULL, NULL, NULL, TRUE, NULL, NULL),
(19014, 'CLCPRO', 'hasCat', 'flag', NULL, NULL, NULL, TRUE, NULL, NULL),
(19014, 'CLCPRO', 'hasBird', 'flag', NULL, NULL, NULL, TRUE, NULL, NULL),
(19014, 'CLCPRO', 'dateOfBirth', 'date', NULL, NULL, NULL, NULL, DATE '2004-07-15', NULL),
(19014, 'CLCPRO', 'yearOfBirth', 'int', NULL, 2004, NULL, NULL, NULL, NULL),
(19014, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-09-21 21:02:04'),
(19014, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-10-17 11:16:21'),
(19014, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-11-30 06:59:41'),
(19014, 'CLCPRO', 'contactCode', 'string', 'RX16', NULL, NULL, NULL, NULL, NULL),
(19014, 'CLCPRO', 'xScore', 'decimal', NULL, NULL, 0.8171633, NULL, NULL, NULL),
(19015, 'CLCPRO', 'hasPet', 'flag', NULL, NULL, NULL, FALSE, NULL, NULL),
(19015, 'CLCPRO', 'yearOfBirth', 'int', NULL, 2000, NULL, NULL, NULL, NULL),
(19016, 'CLCPRO', 'hasPet', 'flag', NULL, NULL, NULL, TRUE, NULL, NULL),
(19016, 'CLCPRO', 'hasDog', 'flag', NULL, NULL, NULL, TRUE, NULL, NULL),
(19016, 'CLCPRO', 'xScore', 'decimal', NULL, NULL, 0.4871632, NULL, NULL, NULL),
(19018, 'ZOMBEE', 'cinema', 'flag', NULL, NULL, NULL, TRUE, NULL, NULL),
(19018, 'ZOMBEE', 'video', 'flag', NULL, NULL, NULL, TRUE, NULL, NULL),
(19018, 'ZOMBEE', 'xScore', 'decimal', NULL, NULL, 0.7871687, NULL, NULL, NULL),
(19019, 'CLCPRO', 'hasPet', 'flag', NULL, NULL, NULL, TRUE, NULL, NULL),
(19019, 'CLCPRO', 'dateOfBirth', 'date', NULL, NULL, NULL, NULL, DATE '2003-08-01', NULL),
(19019, 'CLCPRO', 'yearOfBirth', 'int', NULL, 2003, NULL, NULL, NULL, NULL),
(19019, 'CLCPRO', 'homeCity', 'string', 'Denver', NULL, NULL, NULL, NULL, NULL),
(19019, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-10-19 12:18:37'),
(19019, 'CLCPRO', 'contactCode', 'string', 'RX90', NULL, NULL, NULL, NULL, NULL),
(19019, 'CLCPRO', 'xScore', 'decimal', NULL, NULL, 0.9871621, NULL, NULL, NULL),
(19020, 'CLCPRO', 'hasPet', 'flag', NULL, NULL, NULL, FALSE, NULL, NULL),
(19020, 'CLCPRO', 'homeCity', 'string', 'Chicago', NULL, NULL, NULL, NULL, NULL),
(19020, 'CLCPRO', 'contactTime', 'timestamp', NULL, NULL, NULL, NULL, NULL, TIMESTAMP '2024-10-19 13:17:41'),
(19020, 'CLCPRO', 'contactCode', 'string', 'RX90', NULL, NULL, NULL, NULL, NULL),
(19020, 'CLCPRO', 'xScore', 'decimal', NULL, NULL, 0.7871633, NULL, NULL, NULL);

-- table that contains some fictional survey answers
CREATE TABLE T_SURVEY (
	PID INT NOT NULL,
	TENANT INT NOT NULL,
	Q_KEY VARCHAR(20) NOT NULL,
	A_STR VARCHAR(2000),
	A_INT BIGINT,
	A_YESNO BOOLEAN
);

INSERT INTO T_SURVEY(PID, TENANT, Q_KEY, A_STR, A_INT, A_YESNO)
VALUES
(19011, 17, 'monthlyIncome', NULL, 4600, NULL),
(19011, 17, 'monthlySpending', NULL, 5000, NULL),
(19011, 17, 'martialStatus', 'married', NULL, NULL),
(19011, 17, 'children', NULL, 1, NULL),
(19011, 17, 'favColor', 'blue', NULL, NULL),
(19011, 17, 'favColor', 'yellow', NULL, NULL),
(19011, 17, 'gender', 'female', NULL, NULL),
(19011, 17, 'foodPref', 'fish, mediterrainean', NULL, NULL),
(19011, 17, 'carOwner', NULL, NULL, TRUE),
(19011, 17, 'carColor', 'black', NULL, NULL),
(19011, 17, 'vegetarian', NULL, NULL, FALSE),
(19011, 17, 'vegan', NULL, NULL, FALSE),
(19012, 17, 'monthlyIncome', NULL, 8600, NULL),
(19012, 17, 'monthlySpending', NULL, 6000, NULL),
(19012, 17, 'martialStatus', 'unmarried', NULL, NULL),
(19012, 17, 'children', NULL, 0, NULL),
(19012, 17, 'favColor', 'red', NULL, NULL),
(19012, 17, 'gender', 'male', NULL, NULL),
(19012, 17, 'foodPref', 'steak, burgers', NULL, NULL),
(19012, 17, 'carOwner', NULL, NULL, FALSE),
(19012, 17, 'vegetarian', NULL, NULL, FALSE),
(19012, 17, 'vegan', NULL, NULL, FALSE),
(19013, 17, 'monthlyIncome', NULL, 10000, NULL),
(19013, 17, 'monthlySpending', NULL, 7500, NULL),
(19013, 17, 'martialStatus', 'unmarried', NULL, NULL),
(19013, 17, 'children', NULL, 2, NULL),
(19013, 17, 'favColor', 'red', NULL, NULL),
(19013, 17, 'favColor', 'blue', NULL, NULL),
(19013, 17, 'gender', 'female', NULL, NULL),
(19013, 17, 'foodPref', 'indian', NULL, NULL),
(19013, 17, 'carOwner', NULL, NULL, TRUE),
(19013, 17, 'carColor', 'red', NULL, NULL),
(19013, 17, 'vegetarian', NULL, NULL, TRUE),
(19013, 17, 'vegan', NULL, NULL, TRUE),
(19017, 17, 'martialStatus', 'divorced', NULL, NULL),
(19017, 17, 'children', NULL, 7, NULL),
(19017, 17, 'favColor', 'black', NULL, NULL),
(19017, 17, 'gender', 'female', NULL, NULL),
(19017, 17, 'vegetarian', NULL, NULL, TRUE),
(19017, 17, 'vegan', NULL, NULL, FALSE),
(19018, 17, 'monthlyIncome', NULL, 13500, NULL),
(19018, 17, 'monthlySpending', NULL, 11000, NULL),
(19018, 17, 'martialStatus', 'unmarried', NULL, NULL),
(19018, 17, 'children', NULL, 1, NULL),
(19018, 17, 'favColor', 'red', NULL, NULL),
(19018, 17, 'favColor', 'yellow', NULL, NULL),
(19018, 17, 'favColor', 'black', NULL, NULL),
(19018, 17, 'favColor', 'white', NULL, NULL),
(19018, 17, 'gender', 'male', NULL, NULL),
(19018, 17, 'foodPref', 'chicken, burgers', NULL, NULL),
(19018, 17, 'carOwner', NULL, NULL, TRUE),
(19018, 17, 'carColor', 'yellow', NULL, NULL),
(19018, 17, 'vegetarian', NULL, NULL, FALSE),
(19018, 17, 'vegan', NULL, NULL, FALSE),
(19020, 17, 'favColor', 'black', NULL, NULL),
(19020, 17, 'favColor', 'white', NULL, NULL),
(19020, 17, 'gender', 'male', NULL, NULL),
(19020, 17, 'foodPref', 'thai', NULL, NULL),
(19020, 17, 'vegetarian', NULL, NULL, TRUE),
(19020, 17, 'vegan', NULL, NULL, FALSE),
(19021, 17, 'monthlySpending', NULL, 5000, NULL),
(19021, 17, 'martialStatus', 'unmarried', NULL, NULL),
(19021, 17, 'children', NULL, 0, NULL),
(19021, 17, 'favColor', 'red', NULL, NULL),
(19021, 17, 'favColor', 'green', NULL, NULL),
(19021, 17, 'gender', 'diverse', NULL, NULL),
(19021, 17, 'carOwner', NULL, NULL, TRUE),
(19021, 17, 'carColor', 'white', NULL, NULL),
(19021, 17, 'vegetarian', NULL, NULL, TRUE),
(19021, 17, 'vegan', NULL, NULL, TRUE);

-- table that contains some fictional POS data
CREATE TABLE T_POSDATA (
	UID INT NOT NULL,
	INV_NO INT NOT NULL,
	INV_DATE DATE,
	CODE VARCHAR(20) NOT NULL,
	DESCRIPTION VARCHAR(255),
	QUANTITY INT,
	UNIT_PRICE DECIMAL(10,2),
	COUNTRY VARCHAR(255)
);

INSERT INTO T_POSDATA(UID, INV_NO, INV_DATE, CODE, DESCRIPTION, QUANTITY, UNIT_PRICE, COUNTRY)
VALUES
(19011, 72163, DATE '2024-01-13', '145AB', 'DROP LIGHT', 2, 198.78, 'USA'),
(19011, 32117, DATE '2024-03-17', '82738', 'JELLY BEANS', 1, 1.65, 'USA'),
(19011, 42128, DATE '2024-03-21', '99738', 'SANDWICH', 1, 3.85, 'UK'),
(19011, 22729, DATE '2024-03-29', '1073A', 'TOASTER', 1, 499.99, 'UK'),
(19011, 33721, DATE '2024-03-30', '3373C', 'POSTCARDS', 9, 1.99, 'UK'),
(19011, 32117, DATE '2024-05-18', '11439', 'POTATO WEDGES 1LB', 4, 2.99, 'USA'),
(19012, 72122, DATE '2024-02-14', '166BB', 'TENNIS DRESS', 1, 178.90, 'USA'),
(19012, 32119, DATE '2024-03-17', '22738', 'TOBACCO 1LB', 1, 49.99, 'USA'),
(19012, 55128, DATE '2024-03-21', '11739', 'CHEESEBURGER', 1, 11.85, 'USA'),
(19012, 66720, DATE '2024-03-29', '4443A', 'RENT-A-BUS WEEK PASS', 1, 1699.99, 'USA'),
(19012, 13724, DATE '2024-03-31', '64321', 'POPCORN', 1, 4.99, 'USA'),
(19012, 32117, DATE '2024-04-30', '11439', 'POTATO WEDGES 1LB', 1, 2.99, 'USA'),
(19013, 62192, DATE '2024-02-14', '555BC', 'FRESH FRUIT MIX 1LB', 2, 6.75, 'USA'),
(19013, 22669, DATE '2024-03-15', '14987', 'PUMPKIN', 1, 3.99, 'USA'),
(19013, 33168, DATE '2024-03-21', '15039', 'CORNFLAKES', 1, 3.25, 'USA'),
(19013, 52720, DATE '2024-03-21', '1243A', 'OAT MILK 0.5GAL', 6, 4.89, 'USA'),
(19013, 14725, DATE '2024-03-31', '5521C', 'WATERMELON EXTRACT 1GILL', 1, 149.80, 'USA'),
(19013, 81543, DATE '2024-03-31', '11439', 'POTATO WEDGES 1LB', 1, 2.99, 'USA'),
(19014, 42166, DATE '2024-03-21', '3272K', 'BOUQUET', 1, 33.00, 'GERMANY'),
(19014, 24449, DATE '2024-03-29', '1088B', 'BUS TICKET 3 ZONES', 1, 8.50, 'GERMANY'),
(19014, 16321, DATE '2024-03-30', '1H73C', 'TOILET CLEANER', 1, 3.99, 'GERMANY'),
(19014, 39987, DATE '2024-06-20', 'XX439', 'LIP GLOSS', 2, 19.99, 'GERMANY'),
(19015, 72163, DATE '2024-01-17', '166AB', 'CORGY FOOD 1KG', 2, 1.78, 'UK'),
(19015, 32117, DATE '2024-03-28', '15538', 'BAKED BEANS 250GR', 1, 0.65, 'UK'),
(19015, 42128, DATE '2024-03-22', '99738', 'SANDWICH', 1, 3.85, 'UK'),
(19015, 22729, DATE '2024-04-11', '1073A', 'TOASTER', 1, 499.99, 'UK'),
(19015, 33721, DATE '2024-05-18', '3373C', 'POSTCARDS', 1, 1.99, 'UK'),
(19015, 32117, DATE '2024-05-20', 'K1430', 'MINTY MEATBALLS 1KG', 1, 0.99, 'UK'),
(19017, 41191, DATE '2024-03-14', '115BC', 'RED WINE 0.75L', 6, 5.75, 'FRANCE'),
(19017, 23219, DATE '2024-03-15', '7723D', 'BAGUETTE', 1, 3.99, 'FRANCE'),
(19017, 39967, DATE '2024-03-15', '22039', 'CORNFLAKES', 2, 4.25, 'FRANCE'),
(19017, 23721, DATE '2024-03-15', 'H778A', 'CHEESE 36M 100GR', 1, 8.99, 'FRANCE'),
(19017, 64785, DATE '2024-03-31', '5521C', 'WATERMELON EXTRACT 0.118L', 1, 199.99, 'FRANCE');


-- extreme table with text-only key-value
CREATE TABLE T_FLAT_TXT (
	UID INT NOT NULL,
	C_KEY VARCHAR(100) NOT NULL,
	C_VALUE VARCHAR(255) NOT NULL
);

INSERT INTO T_FLAT_TXT(UID, C_KEY, C_VALUE)
VALUES
(19011, 'sports', 'tennis'),
(19011, 'sports', 'football'),
(19011, 'sizeCM', '178'),
(19011, 'bodyTempCelsius', '38.9'),
(19011, 'anniverseryDate', '2024-09-20'),
(19011, 'clubMember', 'Y'),
(19012, 'sports', 'tennis'),
(19012, 'hobbies', 'train-spotting'),
(19012, 'hobbies', 'origami'),
(19012, 'sizeCM', '172'),
(19012, 'bodyTempCelsius', '37.3'),
(19012, 'anniverseryDate', '2023-03-31'),
(19013, 'hobbies', 'origami'),
(19013, 'sizeCM', '178'),
(19013, 'bodyTempCelsius', '38.7'),
(19013, 'anniverseryDate', '2024-09-25'),
(19013, 'clubMember', 'Y'),
(19014, 'sports', 'football'),
(19014, 'sports', 'basketball'),
(19014, 'sizeCM', '192'),
(19014, 'clubMember', 'N'),
(19015, 'clubMember', 'Y'),
(19016, 'sports', 'tennis'),
(19016, 'sports', 'football'),
(19016, 'sports', 'basketball'),
(19016, 'hobbies', 'train-spotting'),
(19016, 'sizeCM', '176'),
(19016, 'bodyTempCelsius', '37.5'),
(19016, 'anniverseryDate', '2024-12-24'),
(19016, 'clubMember', 'N'),
(19017, 'sports', 'hiking'),
(19017, 'hobbies', 'knitting'),
(19017, 'hobbies', 'origami'),
(19017, 'clubMember', 'Y'),
(19021, 'sports', 'baseball'),
(19021, 'sports', 'football'),
(19021, 'sizeCM', '188'),
(19021, 'bodyTempCelsius', '38.9'),
(19021, 'anniverseryDate', '2024-12-01'),
(19021, 'clubMember', 'Y');





