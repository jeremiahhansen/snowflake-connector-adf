SELECT /*Parameter_With_Quotes*/'John'/*firstName*/ AS FIRST_NAME, 'Doe' AS LAST_NAME, 1 AS AGE;

SELECT CONCAT(/*Parameter_With_Quotes*/'John'/*firstName*/, 'Bar') AS OUTPUT_1, /*Parameter*/1/*age*/ + 100 AS OUTPUT_2;
