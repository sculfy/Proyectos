/*
1. CREAR TABLA DE ivr_detail. 

Vamos a realizar el modelo de datos correspondiente a una IVR de atención al cliente. 
Desde los ficheros ivr_calls, ivr_modules, e ivr_steps crear las tablas con los mismos nombres dentro del dataset keepcoding. 
En ivr_calls encontramos los datos referentes a las llamadas. 
En ivr_modules encontramos los datos correspondientes a los diferentes módulos por los que pasa la llamada. Se relaciona con la tabla de ivr_calls a través del campo ivr_id. 
En ivr_steps encontramos los datos correspondientes a los pasos que da el usuario dentro de un módulo. Se relaciona con la tabla de módulos a través de los campos ivr_id y module_sequence. 
Queremos tener los siguientes campos: 
calls_ivr_id 
calls_phone_number 
calls_ivr_result 
calls_vdn_label 
calls_start_date 
calls_start_date_id 
calls_end_date 
calls_end_date_id 
calls_total_duration 
calls_customer_segment 
calls_ivr_language 
calls_steps_module 
calls_module_aggregation 
module_sequece 
module_name 
module_duration 
module_result 
step_sequence 
step_name 
step_result 
step_description_error 
document_type 
document_identification 
customer_phone 
billing_account_id 
Los campos calls_start_date_id y calls_end_date_id son campos de fecha calculados, del tipo yyyymmdd. Por ejemplo, el 1 de enero de 2023 sería 20230101. 

Entregar el código SQL que generaría la tabla ivr_detail dentro del dataset keepcoding.
*/

CREATE OR REPLACE TABLE keepcoding.ivr_detail
AS  (SELECT
         calls.ivr_id                                                   AS    calls_ivr_id
        ,calls.phone_number                                             AS    calls_phone_number
        ,calls.ivr_result                                               AS    calls_ivr_result
        ,calls.vdn_label                                                AS    calls_vdn_label
        ,calls.start_date                                               AS    calls_start_date
        ,SAFE_CAST(FORMAT_DATE('%Y%m%d', calls.start_date) AS INT64)    AS    calls_start_date_id
        ,calls.end_date                                                 AS    calls_end_date
        ,SAFE_CAST(FORMAT_DATE('%Y%m%d', calls.end_date) AS INT64)      AS    calls_end_date_id
        ,calls.total_duration                                           AS    calls_total_duration
        ,calls.customer_segment                                         AS    calls_customer_segment
        ,calls.ivr_language                                             AS    calls_ivr_language
        ,calls.steps_module                                             AS    calls_steps_module
        ,calls.module_aggregation                                       AS    calls_module_aggregation
        ,modules.module_sequece
        ,modules.module_name
        ,modules.module_duration
        ,modules.module_result
        ,step.step_sequence
        ,step.step_name
        ,step.step_result
        ,step.step_description_error
        ,step.document_type
        ,step.document_identification
        ,step.customer_phone
        ,step.billing_account_id
    FROM `keepcoding.ivr_calls`   calls
    LEFT JOIN   `keepcoding.ivr_modules`  modules   ON  calls.ivr_id            =   modules.ivr_id
    LEFT JOIN   `keepcoding.ivr_steps`    step      ON  modules.ivr_id          =   step.ivr_id
                                                    AND modules.module_sequece  =   step.module_sequece
  )