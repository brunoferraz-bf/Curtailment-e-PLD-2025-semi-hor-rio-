# Curtailment-e-PLD-2025-semi-hor-rio-
Este repositório contém o tratamento de dados de curtailment e PLD (semi-horário), com foco na análise da proporção de curtailment ocorrendo com preços acima do piso regulatório.  O objetivo é apoiar análises de risco, portfólio e decisões econômicas no contexto do mercado de energia elétrica brasileiro.
Os dados de PLD estão disponíveis no formato .csv pelo seu fornecedor oficial (CCEE) em apenas um arquivo (horário), e os dados de Curtailment estão disponíveis pelo seu fornecedor oficial (ONS) mês a mês e classificado por fonte (eólica e solar) no formato .parquet e no formato semi-horário, através dos links abaixo:

PLD 2025 - CCEE: https://pda-download.ccee.org.br/korJMXwpSLGyVlpRMQWduA/content
CURTAILMENT EÓLICA: https://dados.ons.org.br/dataset/restricao_coff_eolica_detail
CURTAILMENT SOLAR: https://dados.ons.org.br/dataset/restricao_coff_fotovoltaica_detail

O cálculo  do curtailment foi definido pela seguinte lógica:

Cálculo do curtailment (créditos ao Bernard Küse - https://www.linkedin.com/in/bernardkusel/)

SE val_geracaolimitada não é nulo ENTÃO
 SE val_geracaoreferencia > val_geracao ENTÃO
 val_geracaoreferencia - val_geracao
 SENÃO
 0 
SENÃO
 0
