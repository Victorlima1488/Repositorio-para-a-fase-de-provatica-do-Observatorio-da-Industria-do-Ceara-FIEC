SELECT
    Localidade,
    COUNT(IDAtracacao) AS Numero_Atracoes,
    (COUNT(IDAtracacao) - 
        (SELECT COUNT(IDAtracacao) FROM atracacao_fato 
         WHERE YEAR(Data_Atracacao) = 2021 AND Localidade = atracacao_fato.Localidade
         AND MONTH(Data_Atracacao) = MONTH(atracacao_fato.Data_Atracacao)) 
    ) AS Variacao_Numero_Atracacao,
    AVG(TEsperaAtracacao) AS Tempo_Espera_Medio,
    AVG(TEstadia) AS Tempo_Atracado_Medio,
    MONTH(Data_Atracacao) AS Mes,
    YEAR(Data_Atracacao) AS Ano
FROM
    atracacao_fato
WHERE
    Localidade IN ('Ceará', 'Nordeste', 'Brasil') AND 
    YEAR(Data_Atracacao) IN (2021, 2023)
GROUP BY
    Localidade, MONTH(Data_Atracacao), YEAR(Data_Atracacao)
ORDER BY
    Localidade, Ano, Mes;