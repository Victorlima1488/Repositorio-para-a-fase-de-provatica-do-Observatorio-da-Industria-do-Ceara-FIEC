-- Criação da tabela atracacao_fato
CREATE TABLE atracacao_fato (
    IDAtracacao INT PRIMARY KEY,
    Tipo_Navegacao VARCHAR(100),
    CDTUP VARCHAR(100),
    IDBerco VARCHAR(100),
    Berco VARCHAR(100),
    Porto VARCHAR(100),
    Apelido VARCHAR(100),
    Complexo_Portuario VARCHAR(100),
    Tipo_Autoridade_Portuaria VARCHAR(100),
    Data_Atracacao DATE,
    Data_Chegada DATE,
    Data_Desatracacao DATE,
    Ano INT,
    Mes INT
);

-- Criação da tabela carga_fato
CREATE TABLE carga_fato (
    IDCarga INT PRIMARY KEY,
    IDAtracacao INT,
    Origem VARCHAR(100),
    Destino VARCHAR(100),
    Tipo_Operacao VARCHAR(100),
    Peso_Liquido FLOAT,
    Ano INT,
    Mes INT
);
