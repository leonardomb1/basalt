-- What the book's examples assume: the connections they read and write, and the
-- PARAMs they reference. A PARAM an example declares itself is left out.
CREATE CONNECTION erp TYPE sqlserver OPTIONS (host = 'sql.internal', database = 'totvs');
CREATE CONNECTION fluig TYPE sqlserver OPTIONS (host = 'fluig.internal', database = 'fluig');
CREATE CONNECTION db TYPE sqlserver OPTIONS (host = 'db.internal', database = 'db');
CREATE CONNECTION sr TYPE starrocks OPTIONS (host = 'sr.internal', database = 'bronze');
CREATE CONNECTION pg TYPE postgres OPTIONS (host = 'pg.internal', database = 'erp');
CREATE CONNECTION conn TYPE postgres OPTIONS (host = 'pg.internal', database = 'app');
CREATE CONNECTION crm TYPE http OPTIONS (base_url = 'https://crm.example.com');
CREATE CONNECTION gh TYPE http OPTIONS (base_url = 'https://api.github.com');
CREATE CONNECTION bank TYPE sftp OPTIONS (host = 'sftp.example.com', user = 'empresa');
CREATE CONNECTION fs TYPE smb OPTIONS (host = 'fileserver.corp.local', share = 'Financeiro');
PARAM since DATE DEFAULT '2026-01-01';
PARAM desde DATE DEFAULT '2026-01-01';
PARAM dia STRING DEFAULT '2026-10-01';
PARAM tbl STRING DEFAULT 'SC5';
PARAM tag STRING DEFAULT 'acme';
PARAM env STRING DEFAULT 'prod';
PARAM cols STRING DEFAULT '';
PARAM tables JSON;
PARAM job JSON;
