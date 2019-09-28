SELECT * FROM agua where cpf = 306647133;
SELECT * FROM `violencia`;
SELECT * FROM `seguranca`;

SELECT count(*) FROM `agua`;
SELECT count(*) FROM `seminario`;
SELECT count(*) FROM `seguranca`;
SELECT count(*) FROM `violencia`;


SELECT count(*) FROM `Pessoa_Fisica`;

SELECT rowid, * FROM Pessoa_Fisica order by  nome_completo,  cpf, rowid  ASC ;

select 
     REPLACE(cidade,'/MS','') as cidade
from Pessoa_Fisica where cpf > 0; 

--Campo Grande/ MS

update Pessoa_Fisica 
    set cidade =   REPLACE(cidade,'Chapadao do sul','Chapadao do Sul')
    --set cidade =   'Campo Grande'
--set cpf =  REPLACE(cpf,'.0','')
--set nome_completo = 'ALINE BALTA VIANNA'
where  
--cidade =  'CSMPO GRANDE';  
rowid > 0  ;

SELECT rowid, cidade FROM Pessoa_Fisica where  cidade like '%vinhema%' ;
SELECT rowid, cidade FROM Pessoa_Fisica  order by cidade asc;

SELECT rowid, cidade FROM Pessoa_Fisica where  rowid > 693  ;

SELECT rowid, cidade FROM Pessoa_Fisica where nome_completo = 'WANDA FALEIROS';

SELECT rowid, * FROM Pessoa_Fisica where cpf ='inf.0';



SELECT * FROM agua where nome_completo = 'Nicolas Nichele';   -- 000.002.251-76
     
--000.000.000-00                                        
--018.605.389-40
-- 18.605.389-40


select rowid, substr('00000000000'||cpf,-11) as cpf FROM Pessoa_Fisica;

SELECT rowid, substr('00000000000'||cpf,-11) as cpf_fotmat , cpf FROM Pessoa_Fisica where  rowid in(981 ) ;
                            

SELECT count(*) as tot, cpf  FROM Pessoa_Fisica 
group by cpf ,nome_completo
HAVING tot = 1
order by  tot,  cpf ASC ;



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


--delete from Pessoa_Fisica where nome_completo =  '06369384178';--
--delete from Pessoa_Fisica where  rowid in(981 ) ;

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
