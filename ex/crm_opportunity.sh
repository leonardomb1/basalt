#!/usr/bin/env bash
# Dynamics 365 / Dataverse: opportunity -> StarRocks bronze.
# Uses the 276 base columns (the 155 computed *name/*yominame formatted columns
# are excluded — they make SELECT * time out on the TDS endpoint).
#   USER_CRM, USER_CRM_PASSWORD, USER_SR, USER_SR_PASSWORD in the environment.
#   ./ex/crm_opportunity.sh ["<extra T-SQL WHERE predicate>"]
set -euo pipefail
BIN="${BASALT_BIN:-./zig-out/bin/basalt}"
WHERE="${1:-}"   # e.g. '[modifiedon] > DATEADD(DAY, -1, GETDATE())'

: "${USER_CRM:?}"; : "${USER_CRM_PASSWORD:?}"; : "${USER_SR:?}"; : "${USER_SR_PASSWORD:?}"
export USER_CRM USER_CRM_PASSWORD USER_SR USER_SR_PASSWORD

SCRIPT="$(cat <<'BSL'
@batch
connection crm = sqlserver
  host = "kaeferbr.crm2.dynamics.com" port = 1433
  database = "ripbr"
  auth = "aad"
  user = env("USER_CRM") password = secret("USER_CRM_PASSWORD")
  tls = "require"

connection sr = starrocks
  fe_host = "10.140.0.7" fe_port = 9030
  be_url = "http://10.140.0.10:8040"
  user = env("USER_SR") password = secret("USER_SR_PASSWORD")
  database = "bronze"

read crm query "SELECT opportunityid,accountiddsc,actualclosedate,actualvalue,actualvalue_base,budgetamount,budgetamount_base,budgetstatus,campaignid,campaigniddsc,captureproposalfeedback,closeprobability,completefinalproposal,completeinternalreview,confirminterest,contactiddsc,cr20e_complexidade,cr20e_tipofarmer,createdby,createdbydsc,createdon,createdonbehalfby,createdonbehalfbydsc,currentsituation,customerid,customeriddsc,customeridtype,customerneed,customerpainpoints,decisionmaker,description,developproposal,discountamount,discountamount_base,discountpercentage,emailaddress,estimatedclosedate,estimatedvalue,estimatedvalue_base,evaluatefit,exchangerate,filedebrief,finaldecisiondate,freightamount,freightamount_base,fut_acaorating,fut_avanco,fut_avanodeproposta,fut_backlog,fut_bt_cockpit,fut_bt_desativanumeracaoproposta,fut_bt_finalizaitemestimativa,fut_cotaodematerialequipamentoterceiros,fut_dataprevistainicio,fut_dataprevistatermino,fut_datarating,fut_datarenovacaocontrato,fut_dataultimofollowup,fut_dc_duracao,fut_dc_margemliquidaperc,fut_dc_metaperc,fut_decisaodiretoria,fut_diretor,fut_dtresprating,fut_dt_aceitejuridico,fut_dt_dataentrega,fut_dt_datafollowup,fut_dt_datarecebimento,fut_dt_datavisita,fut_dt_entregaprimeiraproposta,fut_dt_finalizacaoproposta,fut_dt_inicioproposta,fut_dt_visitatecnica,fut_elaboraodeqqpcpu,fut_farmhunter,fut_filialresponsvel,fut_gerente,fut_gestorresponsavel,fut_graudificuldade,fut_int_contadorcockpit,fut_int_duracaoentrega,fut_int_duracaoprevista,fut_int_idrevisao,fut_int_tempoelaboracao,fut_levantamentodequantidadeeoucomengproje,fut_linkeditais,fut_lk_cockpit,fut_lk_cockpit_gerentegeral,fut_lk_filial,fut_lk_filial_proposta,fut_lk_fornecedorintermediario,fut_lk_gerentegeral,fut_lk_modalidadeprincipal,fut_lk_oportunidadeprincipal,fut_lk_planta,fut_lk_propostaativa,fut_lk_propostaatual,fut_lk_setoratuacao,fut_lk_unidadenegocios,fut_lk_visitarelacionada,fut_mn_margemliquida,fut_mn_margemliquida_base,fut_mn_meta,fut_mn_meta_base,fut_mn_receitaliquida,fut_mn_receitaliquida_base,fut_modalidadedocontrato,fut_modalidade_de_contrato,fut_motivododeclnio,fut_perfiloport,fut_pl_fasevenda,fut_pl_localidadefollowup,fut_pl_statusfollowup,fut_pl_tipoduracao,fut_pl_tiposervico,fut_pl_tipovalor,fut_pontosdaproposta,fut_pontosxavanco,fut_rating,fut_ratinganexo,fut_ratingresultado,fut_ratingtecnico,fut_responsavelcomercial,fut_restries,fut_setoratuacocliente,fut_st_codigoproposta,fut_st_coordenadorpor,fut_st_numeroconcorrencia,fut_tempoparaexecuodaproposta,fut_tx_ultimofollowup,fut_ultimofollowup,fut_valordocontrato,i9_consulta_anexada,i9_data_envio_proposta,i9_data_recebimento_consulta,i9_follow_up,i9_realizado_follow_up,i9_vertical,identifycompetitors,identifycustomercontacts,identifypursuitteam,importsequencenumber,initialcommunication,isrevenuesystemcalculated,kfr_complexidade,kfr_gestorresponsavel,knx_analiseslarisco,knx_coordenadorresponsavel,knx_cronograma,knx_datacategorisation,knx_dataelaborarra,knx_datafechamento,knx_dataorex,knx_datapropostatecnicabpf,knx_dataratingbpf,knx_datarevisaoorex,knx_data_bpf,knx_defesatecnica,knx_descrevaomotivododeclinio,knx_destinatariosemail,knx_draftcomercial,knx_drafttecnico,knx_elaborarqqpppueoudfp,knx_equiperesponsavel,knx_etapadofunildevendas,knx_foirealizadovisita,knx_fotos,knx_fotoscobradas,knx_histogramarecursos,knx_informacoesadicionaisemail,knx_levantamentoquantitativo,knx_linkproposta,knx_metodologiaexecutiva,knx_modalidadedecontratacao,knx_motivododeclinioperdida,knx_ncontrato,knx_numerocr,knx_organograma,knx_probabilidade,knx_quemrealizouavisita,knx_reuniaofechamentopremissas,knx_revisaohistogramarecursos,knx_revisaopremissas,knx_revisaopropostatecnica,knx_revisaoqqpppueoudfp,knx_revisaoslarisco,knx_revisardraftcomercial,knx_statusorex,knx_unidadedenegociosorex,lastonholdtime,modifiedby,modifiedbydsc,modifiedon,modifiedonbehalfby,modifiedonbehalfbydsc,msdyn_forecastcategory,msdyn_gdproptout,msdyn_opportunitygrade,msdyn_opportunitykpiid,msdyn_opportunityscore,msdyn_opportunityscoretrend,msdyn_predictivescoreid,msdyn_scorehistory,msdyn_scorereasons,msdyn_segmentid,msdyn_similaropportunities,need,new_observao,new_pretender,new_prioridade,new_responsavelfu,new_volestimado,onholdtime,opportunityratingcode,originatingleadid,originatingleadiddsc,overriddencreatedon,ownerid,owneriddsc,owneridtype,owningbusinessunit,owningteam,owninguser,parentaccountid,parentcontactid,participatesinworkflow,presentfinalproposal,presentproposal,pricelevelid,priceleveliddsc,pricingerrorcode,prioritycode,processid,proposedsolution,purchaseprocess,purchasetimeframe,pursuitdecision,qualificationcomments,quotecomments,resolvefeedback,salesstage,salesstagecode,schedulefollowup_prospect,schedulefollowup_qualify,scheduleproposalmeeting,sendthankyounote,skippricecalculation,slaid,slainvokedid,stageid,statecode,statuscode,stepid,teamsfollowed,timeline,timespentbymeonemailandmeetings,timezoneruleversionnumber,totalamount,totalamountlessfreight,totalamountlessfreight_base,totalamount_base,totaldiscountamount,totaldiscountamount_base,totallineitemamount,totallineitemamount_base,totallineitemdiscountamount,totallineitemdiscountamount_base,totaltax,totaltax_base,transactioncurrencyid,transactioncurrencyiddsc,traversedpath,utcconversiontimezonecode,versionnumber,crm_moneyformatstring,crm_priceformatstring FROM opportunity" @[where = "__WHERE__", buffer]
  | select *, extraction_timestamp = now()
  | write sr stream_load "crm_opportunity" upsert on opportunityid
BSL
)"
SCRIPT="${SCRIPT/__WHERE__/$WHERE}"
exec "$BIN" run -c "$SCRIPT"
