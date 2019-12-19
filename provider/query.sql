select  rowid,  * from cidadao order by nome_completo , telefone_contato  asc;

--delete from cidadao where  rowid in (115);
--
select  rowid, nome_completo, endereco, cidade , telefone  from cadastro_6970  
where 
rowid > 0
ORDER BY nome_completo asc; 

--6968  -> 11863
select  MAX(rowid) from cadastro_6970; 
select  count(rowid) from cadastro_6970; 
--6968 + 4895

-- cadastro_6970   cadastro_4898
update cadastro_6970 
   set endereco =  upper(endereco)
   -- set cidade =  upper(cidade)
   -- set cidade =  REPLACE(cidade,'_MS','')
   -- set endereco =  REPLACE(endereco,'R.','RUA ')
   -- set telefone =  REPLACE(telefone,'-','')
    --set cidade =  'CAMPO GRANDE'
where 
    --cidade like '%Campo%' ; 
    rowid  > 0 ; 




--select * from cadastro_6970 where sexo like '%Fe%'

--INSERT INTO cadastro_6970 
--SELECT *
--FROM cadastro_4898;


-- rowid, nome_completo, endereco, cidade , telefone 

--DELETE FROM TableName
--WHERE  ID NOT IN () 

select count(*) from  cadastro_4898 ;
delete from cadastro_6970 where rowid in (7259, 7285, 434, 8028, 1433);

SELECT rowid, *  FROM cadastro_6970 where nome_completo = 'JUCIANY OJEDA ROJAS FERREIRA';


SELECT nome_completo,
       COUNT(*) TotalCount
FROM   cadastro_6970
GROUP  BY nome_completo
HAVING COUNT(*) > 1
ORDER  BY COUNT(*) DESC ;


DELETE
FROM
    Mytable
WHERE
    RowID NOT IN (
        SELECT
            MIN(RowID)
        FROM
            cadastro_6970
        GROUP BY
            nome_completo,
            endereco,
            cidade
    )










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
