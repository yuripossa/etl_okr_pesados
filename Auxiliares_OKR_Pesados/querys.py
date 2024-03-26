#region Consulta OKR Pesados
query_okr_pesados = """SELECT SISTEMAORIGEM, 
    EMPRESA,
    CODEMPRESA,
    BANDEIRA,
    DOCCLI,
    CODCLIENTE,
    TIPO_OPERACAO,
    TIPO_VENDA,
    TIPO_DOCUMENTO,
    REPLICATE('0', 6 -LEN(CONCAT(MONTH(DATA_EMISSAO),YEAR(DATA_EMISSAO))))+RTrim(CONCAT(MONTH(DATA_EMISSAO),YEAR(DATA_EMISSAO)))  AS ANOMES,
    DATA_EMISSAO,
    DATA_ENTRADA,
    DOCUMENTO,
    NF_NUMERO,
    CHASSI,
    DEPARTAMENTO,
    OPERACAOCOD,
    OPERACAO,
    CASE WHEN OPERACAOCOD = 'DVE' THEN -1 ELSE 1 END QTDEVENDA,
    TIPO_PRODUTO,
    GRUPO_PRODUTO,
    PRODUTO,
    VLR_LIQUIDO_TOTAL_ITEM as VLR_LIQUIDO_TOTAL_NF,
    VLR_LIQUIDO_TOTAL,
    USUARIO,
    Empresa_Usuario,
    CASE WHEN OPERACAOCOD = 'DVE'  AND NomeEmpresaUsuario <> EMPRESA THEN EMPRESA 
    	 ELSE NomeEmpresaUsuario END AS NomeEmpresaUsuario,
    CASE WHEN Departamento LIKE '%CAMINHÕES%' AND NomeEmpresaUsuario NOT LIKE '%CAMINHÕES%' THEN EMPRESA
         WHEN EMPRESA = NomeEmpresaUsuario THEN EMPRESA      
         ELSE NomeEmpresaUsuario END EMPRESAANALITICA,
         NOME_CLIENTE,         
    CASE WHEN OPERACAO = 'VENDA DE MAQUINAS USADAS' THEN 'USADOS'
    ELSE 'NOVOS' END AS TIPO_VENDA_NV,
    QTDE
    
    FROM

    (

    -- SELEÇÃO DE FATURAMENTO TIPO "SERVIÇOS"
    SELECT	
    		'DEALERNETWF' AS SISTEMAORIGEM, 
    		e.Empresa_Nome as EMPRESA,
    		e.Empresa_Codigo as CODEMPRESA,
    		em.Empresa_Nome as BANDEIRA,
    		pc.Pessoa_DocIdentificador as DOCCLI,
    		pc.Pessoa_Codigo AS CODCLIENTE,
    
    		--*****incluido dia 07/03/2024
    		pc.Pessoa_Nome AS NOME_CLIENTE,
    		--*****

    		CASE tno.NaturezaOperacao_TipoItem WHEN 'P' THEN 'Produto' WHEN 'A' THEN 'Avulso' WHEN 'V' THEN 'Veículo' WHEN 'T' THEN 'Mão de Obra' Else 'Desconhecido' END as TIPO_OPERACAO,
    		'Serviços' as TIPO_VENDA,
    		td.TipoDocumento_Descricao AS TIPO_DOCUMENTO,
    		CAST(nf.NotaFiscal_DataEmissao as date) as DATA_EMISSAO,
    		CAST(nf.NotaFiscal_DataChegada as date) as DATA_ENTRADA,
    		nf.NotaFiscal_Codigo AS DOCUMENTO,
    		nf.NotaFiscal_Numero AS NF_NUMERO,
            	V.Veiculo_Chassi AS CHASSI,
    		dp.Departamento_Descricao AS DEPARTAMENTO,
            	onf.NaturezaOperacao_GrupoMovimento AS OPERACAOCOD,
    		onf.NaturezaOperacao_Descricao as OPERACAO,
    		ts.TipoServico_Descricao as TIPO_PRODUTO,
    				CASE TMO.TMO_Tipo
    			WHEN 'DES' THEN 'Deslocamento'
    			WHEN 'DIA' THEN 'Diagnóstico'
    			WHEN 'LAV' THEN 'Lavagem'
    			WHEN 'MAN' THEN 'Manutenção'
    			WHEN 'MPE' THEN 'Manutenção Periódica'
    			WHEN 'REN' THEN 'Revisão Mecânica'
    			WHEN 'RFU' THEN 'Revisão Funilaria'
    			WHEN 'RME' THEN 'Revisão de Entrega'
    			WHEN 'TEX' THEN 'Trabalho Externo' 
    		END as GRUPO_PRODUTO,
    		tmo.TMO_Descricao as PRODUTO,
    		ni.NotaFiscalItem_Qtde as QTDE,
    		replace(ROUND(NotaFiscalItem_ValorTotal + NotaFiscalItem_ValorAcrescimo,2),'.',',') as VLR_LIQUIDO_TOTAL_ITEM,
            replace(ROUND(NotaFiscalItem_ValorPresenteNF,2),'.',',') as VLR_LIQUIDO_TOTAL,
    		u.Usuario_Nome as USUARIO,
    		u.Usuario_EmpresaCodDefault as Empresa_Usuario,
    		emUsu.Empresa_Nome as NomeEmpresaUsuario
    
    FROM NotaFiscal nf 

    	INNER JOIN NotaFiscalItem ni 
    	ON ni.NotaFiscal_Codigo = nf.NotaFiscal_Codigo

    	INNER JOIN Pessoa pc
    	ON  
    	nf.NotaFiscal_PessoaCod = pc.Pessoa_Codigo

    	INNER JOIN Empresa e
    	ON 
    	e.Empresa_Codigo = nf.NotaFiscal_EmpresaCod

    	INNER JOIN Empresa em
    	ON 
    	em.Empresa_Codigo = e.Empresa_EmpresaCodMatriz

    	INNER JOIN NaturezaOperacao onf 
    	ON
    	onf.NaturezaOperacao_Codigo = nf.NotaFiscal_NaturezaOperacaoCod

    	INNER JOIN NaturezaOperacaoTipoItem tno 
    	ON
    	tno.NaturezaOperacao_Codigo = onf.NaturezaOperacao_Codigo

    	LEFT JOIN TMO 
    	ON 
    	TMO.TMO_Codigo = ni.NotaFiscalItem_TMOCod

    	LEFT JOIN TipoServico ts on ts.TipoServico_Codigo = TMO.TipoServico_Codigo

    	LEFT JOIN Departamento dp on dp.Departamento_Codigo = nf.NotaFiscal_DepartamentoCod

    	LEFT JOIN TipoDocumento td on td.TipoDocumento_Codigo = nf.NotaFiscal_TipoDocumentoCod
        	LEFT JOIN Veiculo V ON V.Veiculo_Codigo = NI.NotaFiscalItem_VeiculoCod
    	LEFT JOIN Usuario u on u.Usuario_Codigo = nf.NotaFiscal_UsuCodVendedor
    	LEFT JOIN EMPRESA emUsu on emUsu.Empresa_Codigo = u.Usuario_EmpresaCodDefault	

    WHERE	NotaFiscal_Movimento = 'S' 
    		and NotaFiscal_Status in ('EMI')
    
    		-- NotaFiscal_Serie <> "1" determina que o tipo de venda é de SERVIÇOS.
    		and onf.NaturezaOperacao_GrupoMovimento = 'VEN'
    		and onf.NaturezaOperacao_Codigo in (select distinct TipoOsEmp_NatOpeCod from TipoOsEmp) 
    		and NaturezaOperacao_Classificacao = 'OUT'
    		and tno.NaturezaOperacao_TipoItem = 'T'
    		and nf.NotaFiscal_Serie <> '1'

    union

    -- SELEÇÃO DE FATURAMENTO TIPO "VEICULOS"
    SELECT	
    		'DEALERNETWF' AS SISTEMAORIGEM, 
    		e.Empresa_Nome as EMPRESA,
    		e.Empresa_Codigo as CODEMPRESA,
    		em.Empresa_Nome as BANDEIRA,
    		pc.Pessoa_DocIdentificador as DOCCLI,
    		pc.Pessoa_Codigo AS CODCLIENTE,
    
    		--*****incluido dia 07/03/2024
    		pc.Pessoa_Nome AS NOME_CLIENTE,
    		--*****
    
    		CASE tno.NaturezaOperacao_TipoItem WHEN 'P' THEN 'Produto' WHEN 'A' THEN 'Avulso' WHEN 'V' THEN 'Veículo' WHEN 'T' THEN 'Mão de Obra' Else 'Desconhecido' END as TIPO_OPERACAO,
    		'Veículos' as TIPO_VENDA,
    		td.TipoDocumento_Descricao AS TIPO_DOCUMENTO,
    		CAST(nf.NotaFiscal_DataEmissao as date) as DATA_EMISSAO,
    		CAST(nf.NotaFiscal_DataChegada as date) as DATA_ENTRADA,
    		nf.NotaFiscal_Codigo AS DOCUMENTO,
    		nf.NotaFiscal_Numero AS NF_NUMERO,
            	V.Veiculo_Chassi AS CHASSI,
    		dp.Departamento_Descricao AS DEPARTAMENTO,
            	onf.NaturezaOperacao_GrupoMovimento AS OPERACAOCOD,
    		onf.NaturezaOperacao_Descricao as OPERACAO,

    		CASE fv.FamiliaVeiculo_Tipo
    			WHEN 'O' THEN 'Comercial Pesado'
    			WHEN 'P' THEN 'passageiro'
    			WHEN 'G' THEN 'Extra Pesado'
    			WHEN 'C' THEN 'Comercial leve'
    			WHEN 'I' THEN 'Importado'
    			WHEN 'M' THEN 'Comercial Médio'
    			WHEN 'A' THEN 'Pesados A'
    			WHEN 'B' THEN 'Pesados B'
    			WHEN 'E' THEN 'Pesados C'
    			WHEN 'D' THEN 'Pesados D'
    			WHEN 'F' THEN 'Extra A'
    		END as TIPO_PRODUTO,
    		fv.FamiliaVeiculo_Descricao AS GRUPO_PRODUTO,
    		v.Veiculo_Descricao as PRODUTO,
    		ni.NotaFiscalItem_Qtde as QTDE,
    		replace(ROUND(NotaFiscalItem_ValorTotal + NotaFiscalItem_ValorAcrescimo,2),'.',',') as VLR_LIQUIDO_TOTAL_ITEM,
            	replace(ROUND(NotaFiscalItem_ValorPresenteNF,2),'.',',') as VLR_LIQUIDO_TOTAL,
    		u.Usuario_Nome as usuario,
    		u.Usuario_EmpresaCodDefault as Empresa_Usuario,
    		emUsu.Empresa_Nome as NomeEmpresaUsuario

    FROM NotaFiscal nf 

    	INNER JOIN NotaFiscalItem ni 
    	ON ni.NotaFiscal_Codigo = nf.NotaFiscal_Codigo

    	INNER JOIN Pessoa pc
    	ON  
    	nf.NotaFiscal_PessoaCod = pc.Pessoa_Codigo

    	INNER JOIN Empresa e
    	ON 
    	e.Empresa_Codigo = nf.NotaFiscal_EmpresaCod

    	INNER JOIN Empresa em
    	ON 
    	em.Empresa_Codigo = e.Empresa_EmpresaCodMatriz

    	LEFT JOIN NaturezaOperacao onf 
    	ON
    	onf.NaturezaOperacao_Codigo = nf.NotaFiscal_NaturezaOperacaoCod

    	LEFT JOIN NaturezaOperacaoTipoItem tno 
    	ON
    	tno.NaturezaOperacao_Codigo = onf.NaturezaOperacao_Codigo

    	LEFT JOIN veiculo v 
    	ON 
    	v.Veiculo_Codigo = ni.NotaFiscalItem_VeiculoCod

    	LEFT JOIN ModeloVeiculo mv 
    	ON
    	mv.ModeloVeiculo_Codigo = v.Veiculo_ModeloVeiculoCod

    	LEFT JOIN Marca mc
    	ON 
    	mc.Marca_Codigo = mv.ModeloVeiculo_MarcaCod

    	LEFT JOIN FamiliaVeiculo fv 
    	ON
    	fv.FamiliaVeiculo_Codigo = (case WHEN v.Veiculo_Status = 'N' then mv.ModeloVeiculo_FamiliaVeiculoCod_Novos else mv.ModeloVeiculo_FamiliaVeiculoCod_Usados END)

    	LEFT JOIN Departamento dp on dp.Departamento_Codigo = nf.NotaFiscal_DepartamentoCod

    	LEFT JOIN TipoDocumento td on td.TipoDocumento_Codigo = nf.NotaFiscal_TipoDocumentoCod

    	LEFT JOIN Usuario u on u.Usuario_Codigo = nf.NotaFiscal_UsuCodVendedor
    	LEFT JOIN EMPRESA emUsu on emUsu.Empresa_Codigo = u.Usuario_EmpresaCodDefault	

    WHERE	NotaFiscal_Movimento = 'S' 
    		and NotaFiscal_Status in ('EMI')
    
    		-- NaturezaOperacao_TipoItem = "V" determina que o tipo de venda é de maquinas.
    		and onf.NaturezaOperacao_GrupoMovimento = 'VEN'
    		and tno.NaturezaOperacao_TipoItem = 'V'
    		--and nf.NotaFiscal_Serie <> '1'

    union

    -- SELEÇÃO DE MOVIMENTAÇÃO MERCEDES BENZ - 'PEÇAS'
    SELECT	
    		'DEALERNETWF' AS SISTEMAORIGEM, 
    		e.Empresa_Nome as EMPRESA,
    		e.Empresa_Codigo as CODEMPRESA,
    		em.Empresa_Nome as BANDEIRA,
    		pc.Pessoa_DocIdentificador as DOCCLI,
    		pc.Pessoa_Codigo AS CODCLIENTE,
    
    		--*****incluido dia 07/03/2024
    		pc.Pessoa_Nome AS NOME_CLIENTE,
    		--*****
    
    		CASE tno.NaturezaOperacao_TipoItem WHEN 'P' THEN 'Produto' WHEN 'A' THEN 'Avulso' WHEN 'V' THEN 'Veículo' WHEN 'T' THEN 'Mão de Obra' Else 'Desconhecido' END as TIPO_OPERACAO,
    		'Peças' as TIPO_VENDA,
    		td.TipoDocumento_Descricao AS TIPO_DOCUMENTO,
    		CAST(nf.NotaFiscal_DataEmissao as date) as DATA_EMISSAO,
    		CAST(nf.NotaFiscal_DataChegada as date) as DATA_ENTRADA,
    		nf.NotaFiscal_Codigo AS DOCUMENTO,
    		nf.NotaFiscal_Numero AS NF_NUMERO,
            	V.Veiculo_Chassi AS CHASSI,
    		dp.Departamento_Descricao AS DEPARTAMENTO,
            	onf.NaturezaOperacao_GrupoMovimento AS OPERACAOCOD,
    		onf.NaturezaOperacao_Descricao as OPERACAO,
    		tp.TipoProduto_Descricao as TIPO_PRODUTO,
    		gp.GrupoProduto_Descricao as GRUPO_PRODUTO,
    		p.Produto_Descricao as PRODUTO,
    		ni.NotaFiscalItem_Qtde as QTDE,
    		replace(ROUND(NotaFiscalItem_ValorTotal + NotaFiscalItem_ValorAcrescimo,2),'.',',') as VLR_LIQUIDO_TOTAL_ITEM,
            	replace(ROUND(NotaFiscalItem_ValorPresenteNF,2),'.',',') as VLR_LIQUIDO_TOTAL,
    		u.Usuario_Nome as usuario,
    		u.Usuario_EmpresaCodDefault as Empresa_Usuario,
    		emUsu.Empresa_Nome as NomeEmpresaUsuario


    FROM NotaFiscal nf 

    	INNER JOIN NotaFiscalItem ni 
    	ON ni.NotaFiscal_Codigo = nf.NotaFiscal_Codigo

    	INNER JOIN Pessoa pc
    	ON  
    	nf.NotaFiscal_PessoaCod = pc.Pessoa_Codigo

    	INNER JOIN Empresa e
    	ON 
    	e.Empresa_Codigo = nf.NotaFiscal_EmpresaCod

    	INNER JOIN Empresa em
    	ON 
    	em.Empresa_Codigo = e.Empresa_EmpresaCodMatriz

    	INNER JOIN NaturezaOperacao onf 
    	ON
    	onf.NaturezaOperacao_Codigo = nf.NotaFiscal_NaturezaOperacaoCod

    	INNER JOIN NaturezaOperacaoTipoItem tno 
    	ON
    	tno.NaturezaOperacao_Codigo = onf.NaturezaOperacao_Codigo

    	INNER JOIN Produto p on p.Produto_Codigo = ni.NotaFiscalItem_ProdutoCod

    	LEFT JOIN TipoProduto tp on tp.TipoProduto_Codigo = p.TipoProduto_Codigo

    	LEFT JOIN GrupoProduto gp on gp.GrupoProduto_Codigo = p.GrupoProduto_Codigo

    	LEFT JOIN Departamento dp on dp.Departamento_Codigo = nf.NotaFiscal_DepartamentoCod

    	LEFT JOIN TipoDocumento td on td.TipoDocumento_Codigo = nf.NotaFiscal_TipoDocumentoCod
        LEFT JOIN Veiculo V ON V.Veiculo_Codigo = NI.NotaFiscalItem_VeiculoCod
    	LEFT JOIN Usuario u on u.Usuario_Codigo = nf.NotaFiscal_UsuCodVendedor
    	LEFT JOIN EMPRESA emUsu on emUsu.Empresa_Codigo = u.Usuario_EmpresaCodDefault	

    WHERE	NotaFiscal_Movimento = 'S' 
    		and NotaFiscal_Status in ('EMI')
    
    		--NaturezaOperacao_TipoItem = "P" determina que o tipo de venda é de peças.
    		and onf.NaturezaOperacao_GrupoMovimento = 'VEN'
    		and tno.NaturezaOperacao_TipoItem = 'P'
    		and nf.NotaFiscal_Serie = '1'

    UNION

    SELECT	
    		'DEALERNETWF' AS SISTEMAORIGEM, 
    		e.Empresa_Nome as EMPRESA,
    		e.Empresa_Codigo as CODEMPRESA,
    		em.Empresa_Nome as BANDEIRA,
    		pc.Pessoa_DocIdentificador as DOCCLI,
    		pc.Pessoa_Codigo AS CODCLIENTE,
    
    		--*****incluido dia 07/03/2024
    		pc.Pessoa_Nome AS NOME_CLIENTE,
    		--*****
    
    		CASE tno.NaturezaOperacao_TipoItem WHEN 'P' THEN 'Produto' WHEN 'A' THEN 'Avulso' WHEN 'V' THEN 'Veículo' WHEN 'T' THEN 'Mão de Obra' Else 'Desconhecido' END as TIPO_OPERACAO,
    		'Veículos' as TIPO_VENDA,
    		td.TipoDocumento_Descricao AS TIPO_DOCUMENTO,
    		CAST(nf.NotaFiscal_DataEmissao as date) as DATA_EMISSAO,
    		CAST(nf.NotaFiscal_DataChegada as date) as DATA_ENTRADA,
    		nf.NotaFiscal_Codigo AS DOCUMENTO,
    		nf.NotaFiscal_Numero AS NF_NUMERO,
            	V.Veiculo_Chassi AS CHASSI,
    		dp.Departamento_Descricao AS DEPARTAMENTO,
            	onf.NaturezaOperacao_GrupoMovimento AS OPERACAOCOD,
    		onf.NaturezaOperacao_Descricao as OPERACAO,

    		CASE fv.FamiliaVeiculo_Tipo
    			WHEN 'O' THEN 'Comercial Pesado'
    			WHEN 'P' THEN 'passageiro'
    			WHEN 'G' THEN 'Extra Pesado'
    			WHEN 'C' THEN 'Comercial leve'
    			WHEN 'I' THEN 'Importado'
    			WHEN 'M' THEN 'Comercial Médio'
    			WHEN 'A' THEN 'Pesados A'
    			WHEN 'B' THEN 'Pesados B'
    			WHEN 'E' THEN 'Pesados C'
    			WHEN 'D' THEN 'Pesados D'
    			WHEN 'F' THEN 'Extra A'
    		END as TIPO_PRODUTO,
    		fv.FamiliaVeiculo_Descricao AS GRUPO_PRODUTO,
    		v.Veiculo_Descricao as PRODUTO,
    		ni.NotaFiscalItem_Qtde as QTDE,
    		replace(ROUND((NotaFiscalItem_ValorTotal + NotaFiscalItem_ValorAcrescimo)*-1,2),'.',',') as VLR_LIQUIDO_TOTAL_ITEM,
            	replace(ROUND((NotaFiscalItem_ValorPresenteNF)*-1,2),'.',',') as VLR_LIQUIDO_TOTAL,
    		u.Usuario_Nome as usuario,
    		u.Usuario_EmpresaCodDefault as Empresa_Usuario,
    		emUsu.Empresa_Nome as NomeEmpresaUsuario

    FROM NotaFiscal nf 

    	INNER JOIN NotaFiscalItem ni 
    	ON ni.NotaFiscal_Codigo = nf.NotaFiscal_Codigo

    	INNER JOIN Pessoa pc
    	ON  
    	nf.NotaFiscal_PessoaCod = pc.Pessoa_Codigo

    	INNER JOIN Empresa e
    	ON 
    	e.Empresa_Codigo = nf.NotaFiscal_EmpresaCod

    	INNER JOIN Empresa em
    	ON 
    	em.Empresa_Codigo = e.Empresa_EmpresaCodMatriz

    	LEFT JOIN NaturezaOperacao onf 
    	ON
    	onf.NaturezaOperacao_Codigo = nf.NotaFiscal_NaturezaOperacaoCod

    	LEFT JOIN NaturezaOperacaoTipoItem tno 
    	ON
    	tno.NaturezaOperacao_Codigo = onf.NaturezaOperacao_Codigo

    	LEFT JOIN veiculo v 
    	ON 
    	v.Veiculo_Codigo = ni.NotaFiscalItem_VeiculoCod

    	LEFT JOIN ModeloVeiculo mv 
    	ON
    	mv.ModeloVeiculo_Codigo = v.Veiculo_ModeloVeiculoCod

    	LEFT JOIN Marca mc
    	ON 
    	mc.Marca_Codigo = mv.ModeloVeiculo_MarcaCod

    	LEFT JOIN FamiliaVeiculo fv 
    	ON
    	fv.FamiliaVeiculo_Codigo = (case WHEN v.Veiculo_Status = 'N' then mv.ModeloVeiculo_FamiliaVeiculoCod_Novos else mv.ModeloVeiculo_FamiliaVeiculoCod_Usados END)

    	LEFT JOIN Departamento dp on dp.Departamento_Codigo = nf.NotaFiscal_DepartamentoCod

    	LEFT JOIN TipoDocumento td on td.TipoDocumento_Codigo = nf.NotaFiscal_TipoDocumentoCod

    	LEFT JOIN Usuario u on u.Usuario_Codigo = nf.NotaFiscal_UsuCodVendedor
    	LEFT JOIN EMPRESA emUsu on emUsu.Empresa_Codigo = u.Usuario_EmpresaCodDefault	

    WHERE	NotaFiscal_Movimento = 'E' 
    		and NotaFiscal_Status in ('EMI')
    		--NaturezaOperacao_TipoItem = "V" determina que o tipo de venda é de maquinas.
    		and onf.NaturezaOperacao_GrupoMovimento = 'DVE'
    		and tno.NaturezaOperacao_TipoItem = 'V'
    		--and nf.NotaFiscal_Serie <> '1'

    --order by 2 
    
    )T1

    --WHERE DATA_EMISSAO >= DATEADD(MONTH, -12, GETDATE())

    WHERE DATA_EMISSAO BETWEEN '2023-10-01' AND CAST(GETDATE() -1 AS DATE)
    		AND TIPO_VENDA = 'Veículos'

    """
#endregion

#region Consulta CPC47
query_cpc47 = """
    WITH

    DATAVENDA AS (

        select
           emp.Empresa_Codigo EmpresaVenda_Codigo,
           emp.Empresa_NomeFantasia as EmpresaVenda_NomeFantasia,
           empven.Empresa_Codigo as EmpresaVendedor_Codigo,
           empven.Empresa_NomeFantasia as EmpresaVendedor_NomeFantasia,
           NOP.NaturezaOperacao_Codigo,
           nop.NaturezaOperacao_GrupoMovimento,
           NOP.NaturezaOperacao_Descricao,
    	   PE.Pessoa_Nome,
           convert(varchar(6), nf.NotaFiscal_DataEmissao, 112) as periodo,
           nf.NotaFiscal_Numero,

           case

                 when nop.NaturezaOperacao_GrupoMovimento = 'DVE'

                 then nfi.NotaFiscalItem_ValorPresenteNF * -1

                 else nfi.NotaFiscalItem_ValorPresenteNF

           end as valorcalc,
           nf.NotaFiscal_ValorTotal,
           MV.ModeloVeiculo_Descricao,
           ve.Veiculo_Chassi,

           nf.NotaFiscal_DataEmissao as Veiculo_DataVenda,
           nf.NotaFiscal_Codigo

    from

           NotaFiscal nf

           inner join

                 NotaFiscalItem nfi on

                 nf.NotaFiscal_Codigo = nfi.NotaFiscal_Codigo

           inner join

                 NaturezaOperacao nop on

                 nop.NaturezaOperacao_Codigo = nf.NotaFiscal_NaturezaOperacaoCod

           inner join

                 Empresa emp on

                 emp.Empresa_Codigo = nf.NotaFiscal_EmpresaCod

           inner join

                 Veiculo ve on

                 ve.Veiculo_Codigo = nfi.NotaFiscalItem_VeiculoCod

           inner join

                 Usuario usu on

                 usu.Usuario_Codigo = nf.NotaFiscal_UsuCodVendedor

           inner join

                 Empresa empven on

                 empven.Empresa_Codigo = usu.Usuario_EmpresaCodDefault
                 LEFT JOIN Veiculo V ON V.Veiculo_Codigo = NFI.NotaFiscalItem_VeiculoCod
            LEFT JOIN ModeloVeiculo mv ON MV.ModeloVeiculo_Codigo = V.Veiculo_ModeloVeiculoCod
    		LEFT JOIN pessoa PE ON nf.NotaFiscal_PessoaCod = PE.Pessoa_Codigo 

    where

           nf.NotaFiscal_Status = 'EMI'

           and cast(nf.NotaFiscal_DataEmissao as date) between '2023-10-01' and CAST(GETDATE()-1 AS DATE)

           and emp.Empresa_EmpresaCodMatriz in (1,24)

           and nfi.NotaFiscalItem_TipoItem = 'V'

           and nop.NaturezaOperacao_GrupoMovimento in ('VEN', 'DVE')
    ),

    DATAENTREGA AS (
            select

                        p.Proposta_Codigo,

                        e.PropostaHistorico_Data as Veiculo_DataEntrega,
                         convert(varchar(6), PropostaHistorico_Data, 112)  MesAnoEntrega,
                        p.Proposta_NotaFiscalCod,
                        PropostaHistorico_Codigo StatusProposta


                 from

                        PropostaHistorico e

                        inner join

                              Proposta p on

                              p.Proposta_Codigo = e.Proposta_Codigo

              -- WHERE cast(e.PropostaHistorico_Data as date) between '2023-10-01' and CAST(GETDATE()-1 AS DATE)

    ),

    MAXDATAPROPOSTA AS (
                select

                        E.Proposta_Codigo,
                        P.Proposta_NotaFiscalCod,
                       -- PropostaHistorico_Codigo,
                        MAX(PropostaHistorico_Data) DATASTATUS                    

                 from

                        PropostaHistorico e
                        inner join

                              Proposta p on

                              p.Proposta_Codigo = e.Proposta_Codigo



                              GROUP BY E.Proposta_Codigo,Proposta_NotaFiscalCod--,PropostaHistorico_Codigo

    )

    SELECT  EmpresaVenda_Codigo,
    EmpresaVenda_NomeFantasia,
    EmpresaVendedor_Codigo,
    EmpresaVendedor_NomeFantasia,
    NaturezaOperacao_Codigo,
    NaturezaOperacao_GrupoMovimento,
    NaturezaOperacao_Descricao,
    periodo,
    NotaFiscal_Numero,
    Veiculo_Chassi,
    MesAnoEntrega,
    DATAENTREGA.Proposta_Codigo,
    NotaFiscal_Codigo,
    StatusProposta,
    CASE WHEN (StatusProposta = 'DEV') THEN 'FATURADOMES' 
     WHEN (PERIODO <> MESANOENTREGA AND (StatusProposta = 'FIM' OR StatusProposta = 'ENT')) THEN 'CPC47'
     WHEN (PERIODO = MESANOENTREGA AND (StatusProposta = 'FIM' OR StatusProposta = 'ENT')) THEN 'FATURADOMES'
     WHEN (PERIODO <> MESANOENTREGA AND (StatusProposta <> 'FIM' OR StatusProposta <> 'ENT')) THEN 'CPC47'
     WHEN (PERIODO = MESANOENTREGA AND (StatusProposta <> 'FIM' OR StatusProposta <> 'ENT')) THEN 'CPC47'     
     ELSE 'FATURADOMES' END 'CPC47',
     Veiculo_DataVenda,
     Veiculo_DataEntrega,
     ModeloVeiculo_Descricao,
     REPLACE(NotaFiscal_ValorTotal,'.',',') as ValorTotal,
     CASE WHEN (StatusProposta = 'FIM' OR StatusProposta = 'ENT') THEN 0 
         WHEN (StatusProposta = 'DEV') THEN 0
         ELSE 1 END Qtde_Pendente,
    convert(varchar(6),GETDATE(), 112) as periodoABERTO,
    REPLACE(valorcalc,'.',',') AS valorcalc,
    CASE WHEN (StatusProposta = 'FIM' OR StatusProposta = 'ENT') THEN 'ENTREGUE' 
         WHEN (StatusProposta = 'DEV') THEN 'DEVOLUCAO'
         WHEN (StatusProposta = 'LIB') THEN 'AUTORIZADO'
         ELSE 'PENDENTE' END StatusEntrega,
    Pessoa_Nome AS NOME_CLIENTE
    FROM DATAVENDA
    LEFT JOIN DATAENTREGA ON DATAVENDA.NotaFiscal_Codigo = DATAENTREGA.Proposta_NotaFiscalCod
    JOIN MAXDATAPROPOSTA  M ON M.Proposta_NotaFiscalCod = DATAVENDA.NotaFiscal_Codigo AND M.DATASTATUS = DATAENTREGA.Veiculo_DataEntrega

"""
#endregion

#region Consulta Tabel Relatorio
consultaRelatorioOKR = """
	WITH 
	PRINCIPAL AS (
	    SELECT  OKR.sistemaorigem,
	        empresa,
	        OKR.codempresa,
	        BAN.bandeira,
	        doccli,
	        codcliente,
	        nome_cliente,
	        tipo_operacao,
	        tipo_venda,
	        tipo_documento,
	        anomes,
	        data_emissao,
	        extract(MONTH FROM data_emissao) mes,
	        extract(YEAR FROM data_emissao) ano,
	        data_entrada,
	        documento,
	        nf_numero,
	        chassi,
	        departamento,
	        operacaocod,
	        operacao,
	        qtdevenda,
	        tipo_produto,
	        grupo_produto,
	        produto,
	        qtde,
	        vlr_liquido_total_nf,
	        vlr_liquido_total,
	        usuario,
	        empresa_usuario,
	        nomeempresausuario,
	        empresaanalitica,
	        extract(YEAR FROM data_emissao) || lpad(cast(extract(MONTH FROM data_emissao) as varchar),2,'0') periodo,
	        M.PERIODO PERIODOMETA,
	        SUM(M.QUANTIDADE) quantidade,
	        sum(m.valor) valor,
	        ROW_NUMBER() OVER(PARTITION BY OKR.empresaanalitica, M.periodo ORDER BY OKR.empresaanalitica,M.periodo) RK
	FROM STAGING.dim_okr_pesados OKR
	LEFT JOIN dbdwcorporativo.dim_bandeirasgrupobamaq BAN ON BAN.nomeexibicao = OKR.empresaanalitica
	LEFT JOIN dbdwcorporativo.bqtb_metas_objetivos_valores M ON M.cod_empresa = BAN.codempresa 
	     AND M.periodo = cast((extract(YEAR FROM data_emissao) || lpad(cast(extract(MONTH FROM data_emissao) as varchar),2,'0')) as int) 
	     AND M.metas_objetivos_id BETWEEN 23 AND 28
	WHERE BAN.codmatriz IN (1,24)
	group by 
	OKR.sistemaorigem,
	        empresa,
	        OKR.codempresa,
	        BAN.bandeira,
	        doccli,
	        codcliente,
	        nome_cliente,
	        tipo_operacao,
	        tipo_venda,
	        tipo_documento,
	        anomes,
	        data_emissao,
	        extract(MONTH FROM data_emissao) ,
	        extract(YEAR FROM data_emissao) ,
	        data_entrada,
	        documento,
	        nf_numero,
	        chassi,
	        departamento,
	        operacaocod,
	        operacao,
	        qtdevenda,
	        tipo_produto,
	        grupo_produto,
	        produto,
	        qtde,
	        vlr_liquido_total_nf,
	        vlr_liquido_total,
	        usuario,
	        empresa_usuario,
	        nomeempresausuario,
	        empresaanalitica,
	        (extract(YEAR FROM data_emissao) || lpad(cast(extract(MONTH FROM data_emissao) as varchar),2,'0')) ,
	        M.PERIODO 
	)
	
	    SELECT  sistemaorigem,
	        empresa,
	        codempresa,
	        bandeira,
	        doccli,
	        codcliente,
	        nome_cliente,
	        tipo_operacao,
	        tipo_venda,
	        tipo_documento,
	        anomes,
	        data_emissao,
	        extract(MONTH FROM data_emissao) mes_REAL,
	        extract(YEAR FROM data_emissao) ano_REAL,
	        data_entrada,
	        documento,
	        nf_numero,
	        chassi,
	        departamento,
	        operacaocod,
	        operacao,
	        qtdevenda,
	        tipo_produto,
	        grupo_produto,
	        produto,
	        qtde,
	        vlr_liquido_total_nf,
	        vlr_liquido_total,
	        usuario,
	        empresa_usuario,
	        nomeempresausuario,
	        empresaanalitica,
	        periodo,
	        PERIODO periodo_meta,
	        CAST(CASE WHEN RK = 1 THEN QUANTIDADE ELSE NULL END AS INT) QUANTIDADE,
	        REPLACE(CAST(CASE WHEN RK = 1 THEN VALOR ELSE NULL END AS VARCHAR),'.',',') VALOR,
	        RK
	        FROM PRINCIPAL
"""
#endregion

#region Deletes
deleteOkrPesados = """
	DELETE FROM staging.dim_okr_pesados
"""

deleteCpc47 = """
	DELETE FROM staging.dim_okr_cpc47_pesados
"""
deleteRelOKRPesados = """
	DELETE FROM staging.rlt_okr_pesados
"""
#endregion