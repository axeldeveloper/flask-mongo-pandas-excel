--SELECT * FROM `agua`;
SELECT * FROM `violencia`;
SELECT * FROM `seguranca`;

--SELECT count(*) FROM `agua`;
--SELECT count(*) FROM `seminario`;
--SELECT count(*) FROM `seguranca`;
--SELECT count(*) FROM `violencia`;


SELECT count(*) FROM `Pessoa_Fisica`;

SELECT rowid, * FROM Pessoa_Fisica order by cpf , nome_completo ;

-- SELECT rowid, * FROM tbl1 WHERE letter = 'B'
SELECT * FROM violencia where nome_completo = 'Nicolas Nichele';
SELECT * FROM agua where nome_completo = 'Nicolas Nichele';   -- 000.002.251-76
                                                                         


--INSERT INTO Pessoa_Fisica 
--SELECT *
--FROM seminario;


INSERT INTO Pessoa_Fisica( 
    email,
    cpf               ,
    nome_completo     ,
    aniversario       ,
    endereco          ,
    numero_residencia ,
    complemento       ,
    bairro            ,
    cidade            ,
    telefone_contato  ,
    profissao)
SELECT 
    email,
    cpf               ,
    nome_completo     ,
    aniversario       ,
    endereco          ,
    numero_residencia ,
    complemento       ,
    bairro            ,
    cidade            ,
    telefone_contato  ,
    profissao   
FROM seminario;


--delete from Pessoa_Fisica where nome_completo =  '06369384178';
--delete from Pessoa_Fisica where  rowid = 288;

/*
email             TEXT,
    cpf               REAL,
    nome_completo     TEXT,
    aniversario       TEXT,
    endereco          TEXT,
    numero_residencia INTEGER,
    complemento       TEXT,
    bairro            TEXT,
    cidade            TEXT,
    telefone_contato  INTEGER,
    profissao         TEXT

*/