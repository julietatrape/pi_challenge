drop_table = """
DROP TABLE IF EXISTS {};
"""

create_table = """
CREATE TABLE {}(
    CHROM VARCHAR(25)
    ,POS BIGINT
    ,ID VARCHAR(25)
    ,REF VARCHAR(25)
    ,ALT VARCHAR(25)
    ,QUAL FLOAT
    ,FILTER VARCHAR(25)
    ,INFO VARCHAR(MAX) 
    ,FORMAT VARCHAR(MAX)
    ,MUESTRA BIGINT
    ,VALOR VARCHAR(MAX)
    ,ORIGEN VARCHAR(MAX)
    ,FECHA_COPIA DATE
    ,RESULTADO VARCHAR(25)
);
"""

truncate_table = """
IF OBJECT_ID('{}', 'U') IS NOT NULL
  TRUNCATE TABLE {};
"""

insert_into_stg = """
/*
**********************************************************************************
Se usa la función ventana ROW_NUMBER para evitar duplicados en los datos entrantes
A modo de muestra, se ordena cada partición por el campo "MUESTRA", pero si fuera
necesario se podría(n) tomar otro(s) campo(s) para tal fin
**********************************************************************************
*/

WITH non_duplicates AS(
SELECT
    *
    ,ROW_NUMBER() OVER (PARTITION BY ID, MUESTRA, RESULTADO ORDER BY MUESTRA DESC) AS ROW_NUM 
FROM nuevos_datos_sql
)

INSERT INTO {} 
SELECT
    CHROM
    ,POS
    ,ID
    ,REF
    ,ALT
    ,QUAL
    ,FILTER
    ,INFO
    ,FORMAT
    ,MUESTRA
    ,VALOR
    ,ORIGEN
    ,GETDATE() AS FECHA_COPIA
    ,RESULTADO
FROM non_duplicates
WHERE ROW_NUM = 1;
"""

merge_with_base = """
MERGE INTO {} AS target
USING {} AS source
ON target.ID = source.ID
    AND target.MUESTRA = source.MUESTRA
    AND target.RESULTADO = source.RESULTADO
WHEN MATCHED THEN
    UPDATE SET
        target.CHROM  = source.CHROM
        ,target.POS = source.POS
        ,target.REF = source.REF
        ,target.ALT = source.ALT
        ,target.QUAL = source.QUAL
        ,target.FILTER = source.FILTER
        ,target.INFO = source.INFO
        ,target.FORMAT = source.FORMAT
        ,target.VALOR = source.VALOR
        ,target.ORIGEN = source.ORIGEN
        ,target.FECHA_COPIA = source.FECHA_COPIA
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        CHROM
        ,POS
        ,ID
        ,REF
        ,ALT
        ,QUAL
        ,FILTER
        ,INFO
        ,FORMAT
        ,MUESTRA
        ,VALOR
        ,ORIGEN
        ,FECHA_COPIA
        ,RESULTADO
    )
    VALUES (
        source.CHROM
        ,source.POS
        ,source.ID
        ,source.REF
        ,source.ALT
        ,source.QUAL
        ,source.FILTER
        ,source.INFO
        ,source.FORMAT
        ,source.MUESTRA
        ,source.VALOR
        ,source.ORIGEN
        ,GETDATE()
        ,source.RESULTADO);
"""