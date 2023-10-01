/*
3. CREAR FUNCIÓN DE LIMPIEZA DE ENTEROS 

Crear una función de limpieza de enteros por la que si entra un null la función devuelva el valor -999999. 

Entregar el código SQL que generaría la función clean_integer dentro del dataset keepcoding.
*/

CREATE OR REPLACE FUNCTION keepcoding.clean_integer (p_integer INT64) RETURNS INT64
AS (
    (SELECT
      CASE  WHEN SAFE_CAST(p_integer AS INT64) IS NULL THEN  -999999 
            ELSE SAFE_CAST(p_integer AS INT64) END)
    );

SELECT
  keepcoding.clean_integer (null)