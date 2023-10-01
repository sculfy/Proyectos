/*
2. CREAR TABLA DE ivr_summary 

Con la base de la tabla ivr_detail vamos a crear la tabla ivr_sumary. Ésta será un resumen de la llamada donde se incluyen los indicadores más importantes de 
la llamada. Por tanto, sólo tendrá un registro por llamada. 
Queremos que tengan los siguientes campos: 

ivr_id: identificador de la llamada (viene de detail). 
phone_number: número llamante (viene de detail). 
ivr_result: resultado de la llamada (viene de detail). 
vdn_aggregation: es una generalización del campo vdn_label. Si vdn_label empieza por ATC pondremos FRONT, si empieza por TECH pondremos TECH si es ABSORPTION dejaremos ABSORPTION y si no es ninguna de las anteriores pondremos RESTO. 
start_date: fecha inicio de la llamada (viene de detail). 
end_date: fecha fin de la llamada (viene de detail). 
total_duration: duración de la llamada (viene de detail). 
customer_segment: segmento del cliente (viene de detail). 
ivr_language: idioma de la IVR (viene de detail). 
steps_module: número de módulos por los que pasa la llamada (viene de detail). 
module_aggregation: lista de módulos por los que pasa la llamada (viene de detail. 
document_type: en ocasiones es posible identificar al cliente en alguno de los pasos de detail usar el campo con el mismo nombre en detail. 
document_identification: en ocasiones es posible identificar al cliente en alguno de los pasos de detail usar el campo con el mismo nombre en detail. 
customer_phone: en ocasiones es posible identificar al cliente en alguno de los pasos de detail usar el campo con el mismo nombre en detail. 
billing_account_id: en ocasiones es posible identificar al cliente en alguno de los pasos de detail usar el campo con el mismo nombre en detail. 
masiva_lg: si una llamada pasa por el módulo con nombre AVERIA_MASIVA se indicará con un 1 en este flag, de lo contrario llevará un 0. 
info_by_phone_lg: si una llamada pasa por el step de nombre CUSTOMERINFOBYPHONE.TX y su step_description_error es NULL, quiere decir que hemos podido identificar al cliente a través de su número de teléfono. 
En ese caso pondremos un 1 ente flag, de lo contrario llevará un 0. 
info_by_dni_lg: si una llamada pasa por el step de nombre CUSTOMERINFOBYDNI.TX y su step_description_error es NULL, quiere decir que hemos podido identificar al cliente a través de su número de DNI. En 
ese caso pondremos un 1 ente flag, de lo contrario llevará un 0. 
repeated_phone_24H: es un flag (0 o 1) que indica si ese mismo número ha realizado una llamada en las 24h anteriores. 
cause_recall_phone_24H: es un flag (0 o 1) que indica si ese mismo número ha realizado una llamada en las 24h posteriores. 

Entregar el código SQL que generaría la tabla ivr_summary dentro del dataset keepcoding. 
*/

CREATE OR REPLACE TABLE keepcoding.ivr_summary
AS  (
  WITH

  detail        AS      (SELECT
                           calls_ivr_id                                                                         AS  ivr_id
                          ,calls_phone_number                                                                   AS  phone_number
                          ,calls_ivr_result AS ivr_result
                          ,CASE WHEN calls_vdn_label LIKE 'ATC%'    THEN 'FRONT'
                                WHEN calls_vdn_label LIKE 'TECH%'   THEN 'TECH'
                                WHEN calls_vdn_label = 'ABSORPTION' THEN 'ABSORPTION'
                                ELSE 'RESTO' END                                                                AS  vdn_aggregation
                          ,calls_start_date                                                                     AS  start_date
                          ,calls_end_date                                                                       AS  end_date
                          ,calls_total_duration                                                                 AS  total_duration
                          ,calls_customer_segment                                                               AS  customer_segment
                          ,calls_ivr_language                                                                   AS  ivr_language
                          ,calls_steps_module                                                                   AS  steps_module
                          ,calls_module_aggregation                                                             AS  module_aggregation
                          ,IFNULL(MAX(NULLIF(document_type,'NULL')),'N/A')                                      AS  document_type
                          ,IFNULL(MAX(NULLIF(document_identification,'NULL')),'N/A')                            AS  document_identification
                          ,IFNULL(MAX(NULLIF(customer_phone,'NULL')),'N/A')                                     AS  customer_phone
                          ,IFNULL(MAX(NULLIF(billing_account_id,'NULL')),'N/A')                                 AS  billing_account_id
                          ,MAX(IF(module_name ='AVERIA_MASIVA',1,0))                                            AS  masiva_lg
                          ,MAX(IF(step_name ='CUSTOMERINFOBYPHONE.TX' and step_description_error ='NULL',1,0))  AS  info_by_phone_lg
                          ,MAX(IF(step_name ='CUSTOMERINFOBYDNI.TX' and step_description_error ='NULL',1,0))    AS  info_by_dni_lg
                          ,IF(LAG(DATETIME_TRUNC(calls_start_date, DAY)) OVER (PARTITION BY calls_phone_number ORDER BY calls_phone_number,DATETIME_TRUNC(calls_start_date, DAY) ASC) 
                                = DATETIME_ADD(DATETIME_TRUNC(calls_start_date, DAY), INTERVAL -1 DAY) 
                                ,1,0
                              )                                                                                 AS  repeated_phone_24H
                          ,IF(LEAD(DATETIME_TRUNC(calls_start_date, DAY)) OVER (PARTITION BY calls_phone_number ORDER BY calls_phone_number,DATETIME_TRUNC(calls_start_date, DAY) ASC) 
                                = DATETIME_ADD(DATETIME_TRUNC(calls_start_date, DAY), INTERVAL 1 DAY) 
                                ,1,0
                              )                                                                                 AS  cause_recall_phone_24H
                        FROM `keepcoding.ivr_detail`
                        GROUP BY  ivr_id, phone_number, ivr_result, vdn_aggregation, start_date, end_date, total_duration, customer_segment, ivr_language, steps_module, module_aggregation
                        )

  SELECT *
  FROM detail
  ORDER BY  ivr_id
)