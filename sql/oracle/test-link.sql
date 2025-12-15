SET ECHO ON
SET FEEDBACK ON
SELECT 'Checking if link exists...' AS status FROM dual;
SELECT db_link, username, host FROM all_db_links WHERE db_link = 'ORIG_DB';
SELECT 'Testing link...' AS status FROM dual;
SELECT * FROM dual@orig_db;
EXIT;
